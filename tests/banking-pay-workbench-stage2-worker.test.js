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
    WeakSet,
    RegExp,
    JSON,
    Error
  };
  vm.runInNewContext(
    `${functionBody('canonicalBankingPayWorkbenchUuid')}\n${functionBody('sanitizeBankingPayWorkbenchSourceBuildDiagnostic')}\n${functionBody('runBankingPayWorkbenchSourceBuildLaneAttempt')}\nthis.runAttempt = runBankingPayWorkbenchSourceBuildLaneAttempt;`,
    context
  );
  return context.runAttempt;
}

function loadActualSbRpcLaneAttempt(fetchImpl) {
  const context = {
    Date,
    Math,
    Number,
    String,
    Boolean,
    Object,
    Array,
    Set,
    Map,
    WeakSet,
    RegExp,
    JSON,
    Error,
    AbortController,
    setTimeout,
    clearTimeout,
    encodeURIComponent,
    fetch: fetchImpl,
    sbHeaders: () => ({})
  };
  vm.runInNewContext(
    `${functionBody('canonicalBankingPayWorkbenchUuid')}\n${functionBody('sanitizeBankingPayWorkbenchSourceBuildDiagnostic')}\n${functionBody('sbRpc')}\n${functionBody('runBankingPayWorkbenchSourceBuildLaneAttempt')}\nthis.runAttempt = runBankingPayWorkbenchSourceBuildLaneAttempt;`,
    context
  );
  return context.runAttempt;
}

function loadDiagnosticSanitizer() {
  const context = { String, Number, Math, Object, Array, WeakSet, RegExp, Error, JSON };
  vm.runInNewContext(
    `${functionBody('sanitizeBankingPayWorkbenchSourceBuildDiagnostic')}\nthis.sanitize = sanitizeBankingPayWorkbenchSourceBuildDiagnostic;`,
    context
  );
  return context.sanitize;
}

function loadParallelismNormalizer() {
  const context = { String, Number };
  vm.runInNewContext(
    `${functionBody('normalizeBankingPayWorkbenchSourceBuildParallelism')}\nthis.normalize = normalizeBankingPayWorkbenchSourceBuildParallelism;`,
    context
  );
  return context.normalize;
}

function loadUuidIdentityHelpers() {
  const context = { String, RegExp };
  vm.runInNewContext(
    `${functionBody('canonicalBankingPayWorkbenchUuid')}\n${functionBody('bankingPayWorkbenchStableWorkerId')}\n${functionBody('bankingPayWorkbenchNudgeSingleFlightKey')}\nthis.helpers = { canonicalBankingPayWorkbenchUuid, bankingPayWorkbenchStableWorkerId, bankingPayWorkbenchNudgeSingleFlightKey };`,
    context
  );
  return context.helpers;
}

function loadScheduledCronEligibility() {
  const context = { String };
  vm.runInNewContext(
    `${functionBody('bankingPayWorkbenchScheduledCronEligible')}\nthis.eligible = bankingPayWorkbenchScheduledCronEligible;`,
    context
  );
  return context.eligible;
}

function loadDiagnosticLogger() {
  const entries = [];
  const context = {
    String,
    Number,
    Math,
    Object,
    Array,
    Set,
    WeakSet,
    RegExp,
    Error,
    JSON,
    Date,
    bankingPayWorkbenchLogsEnabled: () => true,
    console: {
      info: (value) => entries.push(String(value)),
      warn: (value) => entries.push(String(value))
    }
  };
  vm.runInNewContext(
    `${functionBody('sanitizeBankingPayWorkbenchSourceBuildDiagnostic')}\n${functionBody('logBankingPayWorkbenchDiag')}\nthis.logDiagnostic = logBankingPayWorkbenchDiag;`,
    context
  );
  return { logDiagnostic: context.logDiagnostic, entries };
}

const ids = {
  job: '11111111-1111-4111-8111-111111111111',
  build: '22222222-2222-4222-8222-222222222222',
  candidate: '33333333-3333-4333-833a-333333333333',
  session: 'abcdef98-7654-4abc-8def-1234567890ab',
  attempt: '44444444-4444-4444-8444-444444444444',
  nonce: 'aBcDeF12-3456-4aBc-8dEf-1234567890aB'
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
    p_attempt_nonce: ids.nonce.toLowerCase(),
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

test('actual RPC 1 non-success bodies never enter claim-uncertain results, aggregation or logs', async () => {
  const nonce = ids.nonce.toLowerCase();
  const bodyVariants = [
    JSON.stringify({ ok: true, claimed: true, attempt_nonce: nonce }),
    JSON.stringify({ detail: { attempt_nonce: ids.nonce.toUpperCase(), message: `ERR_${nonce}_X` } }, null, 2),
    JSON.stringify({ [`AttemptNonce_${nonce}`]: nonce, result_code: `CLAIM_${nonce}` })
  ];

  for (const body of bodyVariants) {
    let fetchCalls = 0;
    const runAttempt = loadActualSbRpcLaneAttempt(async () => {
      fetchCalls += 1;
      return {
        ok: false,
        status: 502,
        text: async () => body
      };
    });
    const result = await runAttempt({ SUPABASE_URL: 'https://test.invalid' }, baseOptions(undefined));

    assert.equal(fetchCalls, 1);
    assert.equal(result.claimed, false);
    assert.equal(result.execute_called, false);
    assert.equal(result.transport_ok, false);
    assert.equal(result.result_code, 'SOURCE_BUILD_ATTEMPT_CLAIM_UNCERTAIN');
    assert.equal(result.error_code, 'SOURCE_BUILD_ATTEMPT_CLAIM_UNCERTAIN');
    assert.equal(
      result.error_message,
      'The source-build claim outcome is uncertain; durable database recovery owns resolution.'
    );

    const sanitize = loadDiagnosticSanitizer();
    const fulfilledLane = sanitize(result, { maxTextLength: 500 });
    const safeJob = { stage_result: fulfilledLane };
    const passSummary = { result_code: fulfilledLane.result_code, error_message: fulfilledLane.error_message };
    const { logDiagnostic, entries } = loadDiagnosticLogger();
    logDiagnostic({ WORKBENCH_LOGS: true }, 'WORKBENCH_SOURCE_BUILD_TWO_CALL_LANE_RESULT', {
      ...passSummary,
      safe_job: safeJob
    });
    const completeOutput = JSON.stringify({ result, fulfilledLane, safeJob, passSummary, entries });
    assert.doesNotMatch(completeOutput, new RegExp(nonce, 'i'));
    assert.equal(completeOutput.includes(body), false);
  }
});

test('no-claim result codes are restricted to the database-owned allowlist', async () => {
  for (const resultCode of ['NO_CLAIM', 'CANDIDATE_DELETED', 'SESSION_OBSOLETE', 'ATTEMPT_GENERATION_OBSOLETE']) {
    const result = await loadLaneAttempt()({}, baseOptions(async () => ({ ok: true, claimed: false, result_code: resultCode })));
    assert.equal(result.result_code, resultCode);
  }

  const injectedCode = `NO_CLAIM_${ids.nonce}`;
  const injected = await loadLaneAttempt()({}, baseOptions(async () => ({
    ok: true,
    claimed: false,
    result_code: injectedCode,
    [`AttemptNonce_${ids.nonce}`]: ids.nonce
  })));
  assert.equal(injected.result_code, 'NO_CLAIM');
  assert.doesNotMatch(JSON.stringify(injected), new RegExp(ids.nonce, 'i'));
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

test('execute transport loss is not retried and mixed-case nonce diagnostics are recursively redacted', async () => {
  let calls = 0;
  const result = await loadLaneAttempt()({}, baseOptions(async () => {
    calls += 1;
    if (calls === 1) return claim();
    const error = new Error(`timeout for ${ids.nonce.toUpperCase()}`);
    error.name = `RPC_${ids.nonce.toLowerCase()}`;
    error.code = `ERR_${ids.nonce.toUpperCase()}`;
    error.detail = { attempt_nonce: ids.nonce, nested: [`retry ${ids.nonce.toLowerCase()}`] };
    throw error;
  }));
  assert.equal(calls, 2);
  assert.equal(result.execute_called, true);
  assert.equal(result.transport_ok, false);
  assert.equal(result.result_code, 'SOURCE_BUILD_ATTEMPT_EXECUTE_UNCERTAIN');
  assert.equal(result.error_code, 'SOURCE_BUILD_ATTEMPT_EXECUTE_UNCERTAIN');
  assert.equal(result.error.code, 'SOURCE_BUILD_ATTEMPT_EXECUTE_UNCERTAIN');
  assert.match(result.error_message, /\[redacted\]/);
  assert.doesNotMatch(JSON.stringify(result), new RegExp(ids.nonce, 'i'));
});

test('central source-build diagnostic scrubber redacts nonce keys, nested values and mixed case before truncation', () => {
  const sanitize = loadDiagnosticSanitizer();
  const upperNonce = ids.nonce.toUpperCase();
  const result = sanitize({
    message: `${'x'.repeat(700)} nonce=${upperNonce}`,
    code: `ERR_${upperNonce}`,
    name: `NAME_${ids.nonce.toLowerCase()}`,
    detail: {
      attempt_nonce: upperNonce,
      nested: [{ reason: `failed for ${ids.nonce}` }]
    }
  }, { secrets: [ids.nonce], maxTextLength: 500 });
  const serialized = JSON.stringify(result);
  assert.doesNotMatch(serialized, new RegExp(ids.nonce, 'i'));
  assert.match(serialized, /\[redacted\]/);
  assert.ok(result.message.length <= 511);
});

test('complete successful post-claim result is nonce-safe before aggregation and logging', async () => {
  let calls = 0;
  const upperNonce = ids.nonce.toUpperCase();
  const result = await loadLaneAttempt()({}, baseOptions(async () => {
    calls += 1;
    if (calls === 1) return claim();
    return execution({
      stage_status: `DONE_${upperNonce}_X`,
      result_code: `CODE_${upperNonce}_X`,
      next_cursor_json: {
        cursor_kind: `K_${upperNonce}_X`,
        dependency_unit_key: `U_${upperNonce}_X`,
        fact_family: `F_${upperNonce}_X`,
        page_number: 2
      }
    });
  }));

  assert.equal(calls, 2);
  const serialized = JSON.stringify(result);
  assert.doesNotMatch(serialized, new RegExp(ids.nonce, 'i'));
  for (const field of ['stage_status', 'result_code', 'cursor_kind', 'dependency_unit_key', 'fact_family']) {
    assert.match(String(result[field]), /\[redacted\]/i);
  }

  const { logDiagnostic, entries } = loadDiagnosticLogger();
  logDiagnostic({ WORKBENCH_LOGS: true }, 'WORKBENCH_SOURCE_BUILD_TWO_CALL_LANE_RESULT', {
    stage_result: result,
    stage_status: result.stage_status,
    result_code: result.result_code,
    cursor_kind: result.cursor_kind,
    dependency_unit_key: result.dependency_unit_key,
    fact_family: result.fact_family
  });
  assert.equal(entries.length, 1);
  assert.doesNotMatch(entries[0], new RegExp(ids.nonce, 'i'));
});

test('diagnostic scrubber redacts object keys, custom Error fields and underscore-adjacent UUID rejection text', () => {
  const sanitize = loadDiagnosticSanitizer();
  const lowerNonce = ids.nonce.toLowerCase();
  const keyed = sanitize({
    [`AttemptNonce_${ids.nonce}`]: 'value',
    [`field_${ids.nonce}`]: { reason: `nested ${ids.nonce}` }
  }, { secrets: [ids.nonce], maxTextLength: 500 });
  const keyedSerialized = JSON.stringify(keyed);
  assert.doesNotMatch(keyedSerialized, new RegExp(ids.nonce, 'i'));
  assert.ok(Object.keys(keyed).some((key) => key.startsWith('redacted_nonce_field')));
  assert.ok(Object.keys(keyed).some((key) => key.startsWith('redacted_field')));

  const rejected = sanitize(`ERR_${lowerNonce}_X`, { redactUuidTokens: true, maxTextLength: 500 });
  assert.equal(rejected, 'ERR_[redacted-uuid]_X');
  assert.doesNotMatch(rejected, new RegExp(lowerNonce, 'i'));

  const error = new Error(`message ${ids.nonce}`);
  error.name = `NAME_${ids.nonce.toUpperCase()}`;
  error.code = `CODE_${ids.nonce}`;
  error.cause = { technical_message: `cause ${ids.nonce}` };
  error.detail = { reason: `detail ${ids.nonce}` };
  error.custom = [{ response: `custom ${ids.nonce}` }];
  error[`AttemptNonce_${ids.nonce}`] = ids.nonce;
  const errorSerialized = JSON.stringify(sanitize(error, { secrets: [ids.nonce] }));
  assert.doesNotMatch(errorSerialized, new RegExp(ids.nonce, 'i'));
});

test('UUID canonicalization makes session worker, lane and single-flight identities case invariant', () => {
  const helpers = loadUuidIdentityHelpers();
  const lower = ids.session.toLowerCase();
  const upper = ids.session.toUpperCase();
  assert.equal(helpers.canonicalBankingPayWorkbenchUuid(upper), lower);
  assert.equal(helpers.canonicalBankingPayWorkbenchUuid(` ${upper} `), lower);
  assert.equal(helpers.canonicalBankingPayWorkbenchUuid('not-a-uuid'), null);
  assert.equal(
    helpers.bankingPayWorkbenchStableWorkerId('NUDGE', upper),
    helpers.bankingPayWorkbenchStableWorkerId('NUDGE', lower)
  );
  assert.equal(
    helpers.bankingPayWorkbenchStableWorkerId('NUDGE', upper),
    `BANKING_PAY_WORKBENCH:NUDGE:SESSION:${lower}`
  );
  assert.equal(
    helpers.bankingPayWorkbenchNudgeSingleFlightKey(upper),
    helpers.bankingPayWorkbenchNudgeSingleFlightKey(lower)
  );
  assert.equal(
    helpers.bankingPayWorkbenchNudgeSingleFlightKey(upper),
    `BANKING_PAY_WORKBENCH_SESSION_DRAIN:${lower}`
  );
  assert.equal(helpers.bankingPayWorkbenchStableWorkerId('CRON', upper), 'BANKING_PAY_WORKBENCH:CRON:GLOBAL');
  assert.equal(helpers.bankingPayWorkbenchStableWorkerId('NUDGE', upper, 'Custom:Case:Worker'), 'Custom:Case:Worker');
  assert.notEqual(
    helpers.bankingPayWorkbenchStableWorkerId('NUDGE', ids.session),
    helpers.bankingPayWorkbenchStableWorkerId('NUDGE', '99999999-9999-4999-8999-999999999999')
  );
});

test('uppercase candidate and session filters accept canonical PostgreSQL claim identity exactly once', async () => {
  const calls = [];
  const result = await loadLaneAttempt()({}, {
    ...baseOptions(async (_env, fn, args) => {
      calls.push({ fn, args });
      return calls.length === 1
        ? claim()
        : execution({ job_id: ids.job.toUpperCase(), build_id: ids.build.toUpperCase() });
    }),
    sessionId: ids.session.toUpperCase(),
    candidateId: ids.candidate.toUpperCase()
  });

  assert.equal(calls.length, 2);
  assert.equal(calls[0].args.p_session_id, ids.session.toLowerCase());
  assert.equal(calls[0].args.p_candidate_id, ids.candidate.toLowerCase());
  assert.equal(calls[1].args.p_job_id, ids.job.toLowerCase());
  assert.equal(calls[1].args.p_build_id, ids.build.toLowerCase());
  assert.equal(calls[1].args.p_attempt_nonce, ids.nonce.toLowerCase());
  assert.equal(result.ok, true);
  assert.equal(result.execute_called, true);

  let mismatchCalls = 0;
  const mismatch = await loadLaneAttempt()({}, {
    ...baseOptions(async () => {
      mismatchCalls += 1;
      return claim();
    }),
    candidateId: '99999999-9999-4999-8999-999999999999'
  });
  assert.equal(mismatchCalls, 1);
  assert.equal(mismatch.result_code, 'SOURCE_BUILD_ATTEMPT_CLAIM_RESPONSE_INVALID');
  assert.equal(mismatch.execute_called, false);
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
  assert.match(sourceBurst, /const recoveryProbeOnly = sourceDuePreflight\.ok === true && dueQueuedCount <= 0/);
  assert.match(sourceBurst, /recoveryProbeOnly && sourceBuildRecoveryProbeCount > 0/);
  assert.match(sourceBurst, /const laneCount = recoveryProbeOnly\s*\? 1/);
  assert.match(sourceBurst, /recovery_capable_rpc1_required: true/);
  assert.match(drain, /source_build_recovery_probe_count: sourceBuildRecoveryProbeCount/);
  assert.match(sourceBurst, /sanitizeBankingPayWorkbenchSourceBuildDiagnostic\(settledLane\.reason/);
  assert.match(sourceBurst, /redactUuidTokens: true/);
});

test('source-build parallelism defaults fail closed to one while explicit zero and configured values remain supported', () => {
  const settings = functionBody('loadSettingsDefaults');
  const cron = functionBody('bankingPayWorkbenchCronTick');
  const drain = functionBody('drainBankingPayWorkbenchJobs');
  const normalize = loadParallelismNormalizer();
  for (const malformed of [undefined, null, '', ' ', false, true, -1, 33, 1.5, 'nope', {}, []]) assert.equal(normalize(malformed), 1);
  assert.equal(normalize(0), 0);
  assert.equal(normalize('0'), 0);
  assert.equal(normalize(1), 1);
  assert.equal(normalize(4), 4);
  assert.equal(normalize('32'), 32);
  assert.match(settings, /source_build_parallelism: normalizeBankingPayWorkbenchSourceBuildParallelism\(_firstConfiguredValue\(row\.banking_pay_workbench_cron_source_build_parallelism/);
  assert.match(settings, /source_build_parallelism: normalizeBankingPayWorkbenchSourceBuildParallelism\(_firstConfiguredValue\(row\.banking_pay_workbench_nudge_source_build_parallelism/);
  assert.doesNotMatch(settings, /source_build_parallelism:\s*[24],/);
  assert.match(cron, /const sourceBuildParallelism = normalizeBankingPayWorkbenchSourceBuildParallelism\(firstConfiguredValue/);
  assert.match(drain, /const sourceBuildParallelism = normalizeBankingPayWorkbenchSourceBuildParallelism\(/);
  assert.match(cron, /sourceBuildParallelism: 0/);
  assert.match(cron, /source_build_parallelism: 0/);
  assert.match(drain, /sourceBuildParallelism > 0/);
  assert.match(drain, /Math\.min\(sourceBuildParallelism, jobsLeft, rowBoundedJobLimit\)/);
  assert.doesNotMatch(worker, /source_build_parallelism:\s*[24],/);
  assert.doesNotMatch(worker, /budgetProfile === 'NUDGE' \? 4 : 2/);
});

test('cron and nudge preserve database-owned stable worker and lane identities across every outer phase', () => {
  const cron = functionBody('bankingPayWorkbenchCronTick');
  const nudge = functionBody('nudgeBankingPayWorkbenchDrain');
  const drain = functionBody('drainBankingPayWorkbenchJobs');
  const lane = functionBody('runBankingPayWorkbenchSourceBuildLaneAttempt');
  assert.match(cron, /canonicalBankingPayWorkbenchUuid\(requestedSessionId\)/);
  assert.match(cron, /canonicalBankingPayWorkbenchUuid\(requestedCandidateId\)/);
  assert.match(cron, /bankingPayWorkbenchStableWorkerId\(budgetProfile, sessionId, configuredWorkerId\)/);
  assert.match(cron, /workerId: stableWorkerId/);
  assert.match(nudge, /canonicalBankingPayWorkbenchUuid\(requestedSessionId\)/);
  assert.match(nudge, /canonicalBankingPayWorkbenchUuid\(requestedCandidateId\)/);
  assert.match(nudge, /bankingPayWorkbenchNudgeSingleFlightKey\(requestedSessionIdValue\)/);
  assert.match(nudge, /const stableNudgeWorkerId/);
  assert.match(nudge, /workerId: stableNudgeWorkerId/);
  assert.match(nudge, /passthroughOptions\.workerId = stableNudgeWorkerId/);
  assert.match(nudge, /origin:.*AUTO_CONTINUATION/);
  assert.match(nudge, /origin:.*FINAL_CHECK/);
  assert.match(nudge, /lockContentionRetryOptions\.origin = .*LOCK_CONTENTION_RETRY/);
  assert.match(nudge, /origin: 'BANKING_PAY_WORKBENCH_SESSION_NUDGE_GLOBAL_TAIL'[\s\S]*?workerId: 'BANKING_PAY_WORKBENCH:NUDGE:GLOBAL'/);
  assert.match(drain, /const sessionFilterId = canonicalBankingPayWorkbenchUuid/);
  assert.match(drain, /const candidateFilterId = canonicalBankingPayWorkbenchUuid/);
  assert.match(drain, /bankingPayWorkbenchStableWorkerId\(/);
  assert.doesNotMatch(drain, /BANKING_PAY_WORKBENCH:\$\{budgetProfile \|\| 'DEFAULT'\}:\$\{origin\}/);
  assert.match(drain, /SOURCE_BUILD_LANE:\$\{laneIndex \|\| 0\}/);
  assert.match(lane, /const sessionId = canonicalBankingPayWorkbenchUuid/);
  assert.match(lane, /const candidateId = canonicalBankingPayWorkbenchUuid/);
  assert.match(lane, /canonicalBankingPayWorkbenchUuid\(claim\.candidate_id\)/);
  assert.match(lane, /canonicalBankingPayWorkbenchUuid\(execution\.job_id\) === jobId/);
});

test('only the minute schedule owns Banking Pay cron lanes', () => {
  const eligible = loadScheduledCronEligibility();
  assert.equal(eligible('* * * * *'), true);
  assert.equal(eligible('*/5 * * * *'), false);
  assert.equal(eligible(''), false);
  assert.match(worker, /if \(!bankingPayWorkbenchScheduledCronEligible\(cronExpr\)\)/);
  assert.match(worker, /SCHEDULED_BANKING_PAY_WORKBENCH_CRON_NOT_OWNER/);
});

test('database-owned Worker runtime settings are not silently clamped below the configured reconciliation window', () => {
  const cron = functionBody('bankingPayWorkbenchCronTick');
  const drain = functionBody('drainBankingPayWorkbenchJobs');
  assert.match(
    cron,
    /const dbWorkerMaxRuntimeMs = readDrainOption\([^;]*8000,\s*1000,\s*30000\s*\)/
  );
  assert.doesNotMatch(
    cron,
    /const dbWorkerMaxRuntimeMs = readDrainOption\([^;]*8000,\s*1000,\s*8000\s*\)/
  );
  assert.match(
    drain,
    /const dbWorkerMaxRuntimeMs = numberInRange\([\s\S]*?8000,\s*1000,\s*30000\s*\)/
  );
  assert.doesNotMatch(
    drain,
    /const dbWorkerMaxRuntimeMs = numberInRange\([\s\S]*?8000,\s*1000,\s*8000\s*\)/
  );
  assert.match(drain, /dbStatementTimeoutMs - rpcSafetyBufferMs/);
  assert.match(drain, /dbWorkerMaxRuntimeMs \+ rpcSafetyBufferMs/);
  assert.match(drain, /executeTimeoutMs: dbRpcHardCapMs/);
});

test('terminal reconciliation calibration remains statement- and lease-bounded', () => {
  const migration = fs.readFileSync(
    path.resolve(__dirname, '../supabase/migrations/06082026_0440_banking_pay_workbench_terminal_reconciliation_window.sql'),
    'utf8'
  );
  assert.match(migration, /banking_pay_workbench_db_worker_max_runtime_ms\s*=\s*22000/);
  assert.match(migration, /banking_pay_workbench_db_statement_timeout_ms\s*=\s*24000/);
  assert.match(migration, /banking_pay_workbench_db_worker_lease_seconds\s*=\s*25/);
  assert.match(migration, /banking_pay_workbench_rpc_safety_buffer_ms\s*=\s*1000/);
  assert.match(migration, /cron_source_build_parallelism\s*=\s*0/);
  assert.match(migration, /nudge_source_build_parallelism\s*=\s*0/);
  assert.match(migration, /TERMINAL_WINDOW_BASELINE_CONFLICT/);
  assert.match(migration, /Policy X:/);
});

test('accepted four-lane 32-burst profile remains inside the proved transaction envelope', () => {
  const migration = fs.readFileSync(
    path.resolve(__dirname, '../supabase/migrations/06082026_0754_banking_pay_workbench_four_lane_32_burst.sql'),
    'utf8'
  );
  assert.match(migration, /cron_source_build_parallelism\s*=\s*4/);
  assert.match(migration, /nudge_source_build_parallelism\s*=\s*4/);
  assert.match(migration, /cron_source_build_parallel_bursts\s*=\s*32/);
  assert.match(migration, /nudge_source_build_parallel_bursts\s*=\s*32/);
  assert.match(migration, /cron_source_build_runtime_floor_ms\s*=\s*45000/);
  assert.match(migration, /nudge_source_build_runtime_floor_ms\s*=\s*45000/);
  assert.match(migration, /db_worker_max_runtime_ms\s*=\s*22000/);
  assert.match(migration, /db_statement_timeout_ms\s*=\s*24000/);
  assert.match(migration, /db_worker_lease_seconds\s*=\s*25/);
  assert.match(migration, /rpc_safety_buffer_ms\s*=\s*1000/);
  assert.match(migration, /FOUR_LANE_PROFILE_BASELINE_CONFLICT/);
  assert.match(migration, /Policy X authority/);
});

test('runtime version advertises the bounded-scope Stage 2 source marker', () => {
  const version = functionBody('handleVersion');
  assert.match(version, /banking_pay_bounded_scope_stage2/);
  assert.match(version, /V1\.2\.16_STAGE2_RECONCILIATION_WINDOW_20260806/);
  assert.match(version, /7165360304f8ef12b3790078e450ed1d4b128c55/);
});
