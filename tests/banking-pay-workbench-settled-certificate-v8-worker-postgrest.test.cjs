const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const broker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const postgrestOrigin = String(process.env.H1_V8_LOCAL_POSTGREST_URL || '').replace(/\/$/, '');
const targetCount = Number(process.env.H1_V8_LOCAL_TARGET || 0);
const enabled = /^https?:\/\//.test(postgrestOrigin) && [101, 1001, 5000].includes(targetCount);

test('actual Worker orchestration durably completes 101/1,001/5,000 through local PostgREST', {
  skip: !enabled,
  timeout: 15 * 60 * 1000
}, async () => {
  const producerStart = broker.indexOf('async function advanceBankingPayWorkbenchSettledCertificateV8');
  const producerEnd = broker.indexOf('async function bankingPayWorkbenchCronTick', producerStart);
  const queueStart = broker.indexOf('function parseBankingPayWorkbenchCertificateContinuationV8');
  const queueEnd = broker.indexOf('function parseBankingPayWorkbenchDrainWakeMessage', queueStart);
  assert.ok(producerStart >= 0 && producerEnd > producerStart && queueStart >= 0 && queueEnd > queueStart);

  const queued = [];
  const rpcCalls = [];
  const sbRpc = async (_env, name, args) => {
    const response = await fetch(`${postgrestOrigin}/rpc/${encodeURIComponent(name)}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify(args || {})
    });
    const text = await response.text();
    if (!response.ok) {
      throw Object.assign(new Error(`LOCAL_POSTGREST_RPC_FAILED:${name}:${response.status}:${text.slice(0, 500)}`), {
        code: 'LOCAL_POSTGREST_RPC_FAILED'
      });
    }
    const payload = text ? JSON.parse(text) : null;
    rpcCalls.push({ name, payload });
    return payload;
  };
  const env = {
    BANKING_PAY_CONTINUATION_QUEUE: {
      async send(message) { queued.push(structuredClone(message)); }
    }
  };
  const context = vm.createContext({ sbRpc, fetch, TextEncoder, structuredClone, console });
  vm.runInContext([
    broker.slice(producerStart, producerEnd),
    broker.slice(queueStart, queueEnd),
    'globalThis.runSweep=advanceReadyBankingPayWorkbenchCertificatesV8;',
    'globalThis.runProducer=advanceBankingPayWorkbenchSettledCertificateV8;',
    'globalThis.parseWake=parseBankingPayWorkbenchCertificateContinuationV8;'
  ].join('\n'), context);

  const startedAt = Date.now();
  let result = await context.runSweep(env, { limit: 1 });
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.equal(result.processed, 1, JSON.stringify(result));
  let producerResult = result.results[0];
  let deliveryCount = 0;
  while (producerResult.completed !== true) {
    assert.ok(queued.length > 0, `continuation queue empty at delivery ${deliveryCount}`);
    const wake = context.parseWake(queued.shift());
    deliveryCount += 1;
    assert.ok(deliveryCount <= 1024, 'bounded continuation did not terminate');
    producerResult = await context.runProducer(env, {
      sessionId: wake.session_id,
      actorUserId: wake.actor_user_id,
      leaseOwner: wake.lease_owner,
      continuationSequence: wake.continuation_sequence,
      maxAppendPages: 8,
      maxSealSteps: 4,
      maxRuntimeMs: 20000
    });
    assert.equal(producerResult.ok, true, JSON.stringify(producerResult));
  }

  const startResponse = rpcCalls.find(call => call.name === 'pay_workbench_settled_certificate_build_start_v8')?.payload;
  assert.equal(Number(startResponse?.selected_constituent_count), targetCount);
  assert.match(producerResult.certification_id, /^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$/);
  assert.equal(producerResult.maximum_requested_page_size, 256);
  assert.equal(producerResult.maximum_build_emission_rows, 64);
  assert.equal(queued.length, 0, 'a terminal certificate must not leave a successor wake');
  assert.ok(rpcCalls.filter(call => call.name === 'pay_workbench_settled_certificate_build_append_page_v8').length >= Math.ceil(targetCount / 64));
  console.log('H1_V8_WORKER_POSTGREST_RESULT', JSON.stringify({
    target_count: targetCount,
    queue_deliveries: deliveryCount,
    append_calls: rpcCalls.filter(call => call.name === 'pay_workbench_settled_certificate_build_append_page_v8').length,
    elapsed_ms: Date.now() - startedAt,
    completed: true
  }));
});
