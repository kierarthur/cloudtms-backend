const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const worker = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');

function functionBody(name) {
  const markers = [`function ${name}`, `async function ${name}`];
  const start = markers.map((marker) => worker.indexOf(marker)).filter((value) => value >= 0).sort((a, b) => a - b)[0];
  assert.ok(Number.isInteger(start) && start >= 0, `${name} missing`);
  const boundaries = [worker.indexOf('\nfunction ', start + 10), worker.indexOf('\nasync function ', start + 10)].filter((value) => value > start);
  const end = boundaries.length ? Math.min(...boundaries) : worker.length;
  return worker.slice(start, end);
}

test('queue source override cannot widen the embedded producer policy', () => {
  const source = `${functionBody('bankingPayContinuationSourcePolicy')}\n${functionBody('buildBankingPayContinuationMessage')}\nthis.build = buildBankingPayContinuationMessage;`;
  const context = { TextEncoder, Date, Set, Error };
  vm.runInNewContext(source, context);
  const operationId = '11111111-1111-4111-8111-111111111111';

  assert.throws(() => context.build({
    operation_id: operationId,
    operation_type: 'PAYMENT_CORRECTION',
    successor_relation: 'SELF',
    source: 'DUE_SCHEDULE_DISCOVERY'
  }, 'QUEUE_DELIVERY_VALIDATION'), (error) => error && error.code === 'BANKING_PAY_CONTINUATION_EMBEDDED_SOURCE_SCOPE_INVALID');

  const accepted = context.build({
    operation_id: operationId,
    operation_type: 'PAYMENT_CORRECTION',
    successor_relation: 'SELF',
    source: 'PAYMENT_CORRECTION_PLAN'
  }, 'QUEUE_DELIVERY_VALIDATION');
  assert.equal(accepted.operation_type, 'PAYMENT_CORRECTION');
  assert.equal(accepted.successor_relation, 'SELF');
});

test('generic lease release preserves a validated nested SQL future wait', () => {
  const body = functionBody('claimAndAdvanceOneBankingPayOperation');
  assert.match(body, /advancedContinuation\.run_after_utc/);
  assert.match(body, /progressPatch\.run_after_utc = new Date\(advancedRunAfterMs\)\.toISOString\(\)/);
  assert.match(body, /legitimateFutureWait \? 3600 : 60/);
  assert.match(body, /immediateMoreWork = releaseState === 'MORE_WORK' && legitimateFutureWait !== true/);
  assert.ok(body.indexOf('progressPatch.run_after_utc = new Date(advancedRunAfterMs).toISOString()') < body.indexOf("sbRpc(env, 'banking_pay_operation_release_lease'"));
});

test('PAYMENT_CORRECTION reads one exact enabled phase configuration and preserves the configured size', async () => {
  const source = `${functionBody('getBankingPayOperationConfig')}\nthis.getConfig = getBankingPayOperationConfig;`;
  const calls = [];
  const context = {
    URLSearchParams,
    Error,
    Date,
    Math,
    Number,
    Object,
    Array,
    String,
    sbRpc: async () => { throw new Error('generic config RPC must not be used for exact correction phases'); },
    sbFetch: async (_env, url) => {
      calls.push(url);
      return { rows: [{
        operation_type: 'PAYMENT_CORRECTION',
        phase: 'PROCESS_CHUNKS',
        chunk_type: 'CANDIDATE_SCOPE',
        default_chunk_size: 10,
        min_chunk_size: 1,
        max_chunk_size: 25,
        max_advance_ms: 7500,
        lock_seconds: 60,
        enabled: true
      }] };
    }
  };
  vm.runInNewContext(source, context);
  const config = await context.getConfig({ SUPABASE_URL: 'https://test.invalid' }, 'PAYMENT_CORRECTION', {
    phase: 'PROCESS_CHUNKS',
    requireExactEnabled: true
  });
  assert.equal(calls.length, 1);
  assert.equal(config.chunks.correction_process_chunks.chunk_size, 10);
  assert.equal(config.chunks.correction_process_chunks.max_chunk_size, 25);
});

test('PAYMENT_CORRECTION exact phase configuration fails closed when disabled', async () => {
  const source = `${functionBody('getBankingPayOperationConfig')}\nthis.getConfig = getBankingPayOperationConfig;`;
  const context = {
    URLSearchParams,
    Error,
    Date,
    Math,
    Number,
    Object,
    Array,
    String,
    sbRpc: async () => ({}),
    sbFetch: async () => ({ rows: [{
      operation_type: 'PAYMENT_CORRECTION',
      phase: 'PREPARE_SELECTION',
      chunk_type: 'CANDIDATE_SCOPE',
      default_chunk_size: 50,
      min_chunk_size: 1,
      max_chunk_size: 100,
      max_advance_ms: 7500,
      lock_seconds: 60,
      enabled: false
    }] })
  };
  vm.runInNewContext(source, context);
  await assert.rejects(context.getConfig({ SUPABASE_URL: 'https://test.invalid' }, 'PAYMENT_CORRECTION', {
    phase: 'PREPARE_SELECTION',
    requireExactEnabled: true
  }), (error) => error && error.code === 'BANKING_PAY_OPERATION_PHASE_CONFIG_DISABLED');
});

test('correction dispatcher passes the configured phase limit, not a hard-coded 50', () => {
  const body = functionBody('advancePaymentCorrectionOperation');
  assert.match(body, /getBankingPayOperationConfig\(env, 'PAYMENT_CORRECTION'/);
  assert.match(body, /p_limit: correctionPhaseLimit/);
  assert.doesNotMatch(body, /p_limit:\s*50/);
  for (const phase of ['PREPARE_SELECTION', 'EXPAND_WORK', 'PROCESS_CHUNKS', 'FINALISE', 'REFRESH_WORKBENCH']) {
    assert.match(functionBody('getBankingPayOperationConfig'), new RegExp(`phase: '${phase}'`));
  }
});
