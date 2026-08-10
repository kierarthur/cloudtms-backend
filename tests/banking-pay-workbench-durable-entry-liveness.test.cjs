const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const draftStep = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '10082026_1025_banking_pay_source_publication_identity_and_draft_step.sql'),
  'utf8'
);

function functionBody(name) {
  const plain = worker.indexOf(`function ${name}`);
  const asyncMarker = worker.indexOf(`async function ${name}`);
  const start = [plain, asyncMarker].filter((value) => value >= 0).sort((a, b) => a - b)[0];
  assert.ok(Number.isInteger(start), `${name} must exist`);
  const boundaries = [
    worker.indexOf('\nfunction ', start + 10),
    worker.indexOf('\nasync function ', start + 10)
  ].filter((value) => value > start);
  return worker.slice(start, boundaries.length ? Math.min(...boundaries) : worker.length);
}

test('committed Workbench work is durably queued before the optimistic isolate-local nudge', () => {
  const helper = functionBody('scheduleBankingPayWorkbenchDrainWithDurableWake');
  const durableAt = helper.indexOf('await enqueueBankingPayWorkbenchDrainWake');
  const immediateAt = helper.indexOf('nudgeBankingPayWorkbenchDrain');
  assert.ok(durableAt >= 0 && immediateAt > durableAt);
  assert.match(helper, /wake_reason: 'PRE_ENTRY_COMMITTED_WORK'/);
  assert.match(helper, /wake_state: durableWakeEnqueued \? 'DURABLY_QUEUED' : 'FAILED_RETRYABLE'/);
  assert.match(helper, /scheduled: durableWakeEnqueued/);
});

test('queue delivery directly awaits a bounded drain and durably hands off before acknowledgement', () => {
  const queue = functionBody('handleBankingPayContinuationQueue');
  const wakeStart = queue.indexOf('if (workbenchWake)');
  const genericStart = queue.indexOf('const flag = readBankingPayContinuationFlag', wakeStart);
  const branch = queue.slice(wakeStart, genericStart);
  const drainAt = branch.indexOf('await bankingPayWorkbenchCronTick');
  const successorAt = branch.indexOf('await enqueueBankingPayWorkbenchDrainWake');
  const ackAt = branch.indexOf('message.ack()');
  assert.ok(drainAt >= 0 && successorAt > drainAt && ackAt > successorAt);
  assert.doesNotMatch(branch, /nudgeBankingPayWorkbenchDrain/);
  assert.match(branch, /drainResult\.entered !== true/);
  assert.match(branch, /BANKING_PAY_WORKBENCH_DRAIN_WAKE_NO_PROGRESS/);
  assert.match(branch, /wake_reason: 'POST_PASS_MORE_DUE'/);
  assert.match(queue, /message\.retry\(\{ delaySeconds: 5 \}\)/);
});

test('nudge truth distinguishes attachment, entry, progress, completion and failure', () => {
  const nudge = functionBody('nudgeBankingPayWorkbenchDrain');
  for (const state of ['CREATED', 'WAIT_UNTIL_ATTACHED', 'ENTERED', 'PROGRESS', 'COMPLETED', 'FAILED', 'WAIT_UNTIL_ATTACH_FAILED']) {
    assert.match(nudge, new RegExp(`state:?'?\\s*=*\\s*'${state}'|state:\\s*'${state}'`));
  }
  assert.match(nudge, /scheduled: waitUntilUsed/);
  assert.match(nudge, /BANKING_PAY_WORKBENCH_WAIT_UNTIL_FAILED/);
  assert.match(nudge, /scheduled: false/);
  assert.doesNotMatch(nudge, /BANKING_PAY_WORKBENCH_NUDGE_ALREADY_RUNNING_LEGACY_ENTRY/);
});

test('manual refresh, Draft, execute, settlement and cancellation post-actions use durable pre-entry wakes', () => {
  const requiredOrigins = [
    'USER_REQUESTED_FULL_WORKBENCH_REFRESH',
    'DRAFT_CREATE_POST_ACTION_PATCH_TARGETED_REFRESH',
    'DRAFT_CREATE_POST_CREATE_REPLACEMENT',
    'PAYMENT_EXECUTE_POST_ACTION_PATCH_TARGETED_REFRESH',
    'PAYMENT_SETTLE_POST_ACTION_PATCH_TARGETED_REFRESH',
    'PAYMENT_CANCEL_POST_ACTION_PATCH_TARGETED_REFRESH',
    'PAYMENT_CORRECTION_REFRESH_WORKBENCH'
  ];
  for (const origin of requiredOrigins) {
    const at = worker.indexOf(`origin: '${origin}'`);
    assert.ok(at >= 0, `${origin} must exist`);
    const prefix = worker.slice(Math.max(0, at - 240), at + 50);
    assert.match(prefix, /scheduleBankingPayWorkbenchDrainWithDurableWake/);
  }
});

test('Draft SQL authority retains its bounded same-phase loop and is not rewritten by this fix', () => {
  assert.match(draftStep, /v_processed_chunk_count/);
  assert.match(draftStep, /CONTINUE;/);
  assert.match(draftStep, /phase_complete/);
  assert.match(draftStep, /banking_pay_draft_create_step_v1/);
});
