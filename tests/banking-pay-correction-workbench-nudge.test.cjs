const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');

const parserStart = worker.indexOf('function parsePaymentCorrectionWorkbenchNudgeEnvelope');
const schedulerStart = worker.indexOf('async function schedulePaymentCorrectionWorkbenchNudge');
const schedulerEnd = worker.indexOf('async function advancePaymentCorrectionOperation', schedulerStart);
assert.ok(parserStart >= 0 && schedulerStart > parserStart && schedulerEnd > schedulerStart);

const helperSource = worker.slice(parserStart, schedulerEnd);
const ids = {
  operation: '11111111-1111-4111-8111-111111111111',
  correction: '22222222-2222-4222-8222-222222222222',
  session: '33333333-3333-4333-8333-333333333333',
  actor: '44444444-4444-4444-8444-444444444444',
  candidate: '55555555-5555-4555-8555-555555555555',
  job: '77777777-7777-4777-8777-777777777777'
};

function makeEnvelope(overrides = {}) {
  return {
    contract_version: 'PAYMENT_CORRECTION_WORKBENCH_NUDGE_V1',
    nudge_required: true,
    refresh_status: 'STAGED',
    reason: 'PAYMENT_CORRECTION_WORKBENCH_JOBS_STAGED',
    operation_id: ids.operation,
    correction_request_id: ids.correction,
    session_id: ids.session,
    actor_user_id: ids.actor,
    refresh_sequence_no: 1,
    refresh_has_more: false,
    candidate_ids: [ids.candidate],
    candidate_count: 1,
    job_ids: [ids.job],
    job_count: 1,
    source: 'PAYMENT_CORRECTION_REFRESH_WORKBENCH',
    ...overrides
  };
}

function loadHelpers(stubs = {}) {
  const context = {
    nudgeBankingPayWorkbenchDrain: stubs.nudgeBankingPayWorkbenchDrain,
    enqueueBankingPayWorkbenchDrainWake: stubs.enqueueBankingPayWorkbenchDrainWake
  };
  return vm.runInNewContext(
    `${helperSource}\n({ parsePaymentCorrectionWorkbenchNudgeEnvelope, schedulePaymentCorrectionWorkbenchNudge })`,
    context
  );
}

function parse(helpers, envelope) {
  return helpers.parsePaymentCorrectionWorkbenchNudgeEnvelope(
    { workbench_refresh_nudge: envelope },
    { operationId: ids.operation, correctionRequestId: ids.correction }
  );
}

test('strict parser accepts only the canonical database-owned nudge envelope', () => {
  const helpers = loadHelpers();
  const accepted = parse(helpers, makeEnvelope());
  assert.equal(accepted.admitted, true);
  assert.equal(accepted.envelope.session_id, ids.session);
  assert.deepEqual(Array.from(accepted.envelope.candidate_ids), [ids.candidate]);

  const invalidCases = [
    makeEnvelope({ extra_field: true }),
    makeEnvelope({ candidate_ids: ['ABCDEF12-3456-4ABC-8DEF-1234567890AB'] }),
    makeEnvelope({ candidate_ids: [ids.candidate, ids.candidate], candidate_count: 2 }),
    makeEnvelope({ candidate_count: 2 }),
    makeEnvelope({ correction_request_id: ids.operation }),
    makeEnvelope({ refresh_status: 'READY' }),
    makeEnvelope({ nudge_required: false })
  ];
  for (const invalid of invalidCases) assert.equal(parse(helpers, invalid).admitted, false);
});

test('one page containing 100 candidates schedules one grouped session nudge', async () => {
  const candidateIds = Array.from({ length: 100 }, (_, index) => `00000000-0000-4000-8000-${index.toString(16).padStart(12, '0')}`);
  const jobIds = Array.from({ length: 100 }, (_, index) => `99999999-9999-4999-8999-${index.toString(16).padStart(12, '0')}`);
  const envelope = makeEnvelope({ candidate_ids: candidateIds, candidate_count: 100, job_ids: jobIds, job_count: 100 });
  const directCalls = [];
  const wakeCalls = [];
  const helpers = loadHelpers({
    nudgeBankingPayWorkbenchDrain(env, ctx, options) {
      directCalls.push({ env, ctx, options });
      return { ok: true, scheduled: true, wait_until_used: true };
    },
    async enqueueBankingPayWorkbenchDrainWake(env, options) {
      wakeCalls.push({ env, options });
      return { ok: true, enqueued: true };
    }
  });
  const parsed = parse(helpers, envelope);
  assert.equal(parsed.admitted, true);
  const executionContext = { waitUntil() {} };
  const env = { marker: true };
  const result = await helpers.schedulePaymentCorrectionWorkbenchNudge(env, executionContext, parsed.envelope);
  assert.equal(result.scheduled, true);
  assert.equal(result.wait_until_used, true);
  assert.equal(result.durable_wake_enqueued, true);
  assert.equal(directCalls.length, 1);
  assert.equal(directCalls[0].env, env);
  assert.equal(directCalls[0].ctx, executionContext);
  assert.equal(directCalls[0].options.sessionId, ids.session);
  assert.equal(directCalls[0].options.candidateId, undefined);
  assert.equal(wakeCalls.length, 1);
});

test('missing or non-durable execution context falls back to one durable session wake', async () => {
  for (const directMode of ['missing', 'non_durable', 'throws']) {
    const directCalls = [];
    const wakeCalls = [];
    const helpers = loadHelpers({
      nudgeBankingPayWorkbenchDrain(env, ctx, options) {
        directCalls.push({ env, ctx, options });
        if (directMode === 'throws') throw new Error('expected test failure');
        return { ok: true, scheduled: true, wait_until_used: false };
      },
      async enqueueBankingPayWorkbenchDrainWake(env, options) {
        wakeCalls.push({ env, options });
        return { ok: true, enqueued: true };
      }
    });
    const parsed = parse(helpers, makeEnvelope());
    const executionContext = directMode === 'missing' ? null : { waitUntil() {} };
    const result = await helpers.schedulePaymentCorrectionWorkbenchNudge({}, executionContext, parsed.envelope);
    assert.equal(result.scheduled, true);
    assert.equal(result.durable_wake_enqueued, true);
    assert.equal(directCalls.length, directMode === 'missing' ? 0 : 1);
    assert.equal(wakeCalls.length, 1);
    assert.equal(wakeCalls[0].options.session_id, ids.session);
    assert.equal(wakeCalls[0].options.candidate_id, ids.candidate);
  }
});

test('nudge and durable-wake failures reject acknowledgement so durable delivery can retry', async () => {
  const helpers = loadHelpers({
    nudgeBankingPayWorkbenchDrain() {
      throw new Error('expected direct failure');
    },
    async enqueueBankingPayWorkbenchDrainWake() {
      throw new Error('expected queue failure');
    }
  });
  const parsed = parse(helpers, makeEnvelope());
  await assert.rejects(
    helpers.schedulePaymentCorrectionWorkbenchNudge({}, { waitUntil() {} }, parsed.envelope),
    error => error && error.code === 'PAYMENT_CORRECTION_WORKBENCH_DURABLE_WAKE_REQUIRED'
  );
});

test('a database decision that no nudge is required invokes neither wake path', async () => {
  let directCalls = 0;
  let wakeCalls = 0;
  const helpers = loadHelpers({
    nudgeBankingPayWorkbenchDrain() { directCalls += 1; },
    async enqueueBankingPayWorkbenchDrainWake() { wakeCalls += 1; }
  });
  const parsed = parse(helpers, makeEnvelope({
    nudge_required: false,
    refresh_status: 'CURRENT',
    reason: 'PAYMENT_CORRECTION_WORKBENCH_NO_ACTIVE_JOB',
    job_ids: [],
    job_count: 0
  }));
  assert.equal(parsed.admitted, true);
  const result = await helpers.schedulePaymentCorrectionWorkbenchNudge({}, { waitUntil() {} }, parsed.envelope);
  assert.equal(result.scheduled, false);
  assert.equal(result.code, 'PAYMENT_CORRECTION_WORKBENCH_NUDGE_NOT_REQUIRED');
  assert.equal(directCalls, 0);
  assert.equal(wakeCalls, 0);
});
