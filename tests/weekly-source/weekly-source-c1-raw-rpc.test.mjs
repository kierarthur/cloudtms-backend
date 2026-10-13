import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createWeeklySourceC1RawRpc,
  WEEKLY_SOURCE_C1_RAW_RPC_CONTRACT,
} from '../../broker/src/banking-pay/weekly-source-c1-raw-rpc.mjs';

const OPTIONS = Object.freeze({
  automaticRetry: false,
  requestBody: 'LOSSLESS_JSON_TEXT',
  responseBody: 'LOSSLESS_JSON_TEXT',
});

test('sends the exact lossless JSON text once and returns the exact response text', async () => {
  const calls = [];
  const requestText = '{"p_sequence":9223372036854775807,"p_request_json":{"amount_minor":12000}}';
  const responseText = '{"sequence":9223372036854775807,"status":"PUBLISHED"}';
  const rpc = createWeeklySourceC1RawRpc({
    baseUrl: 'https://test-postgrest.invalid/',
    headers: { authorization: 'Bearer test-only' },
    fetchImpl: async (...args) => {
      calls.push(args);
      if (args[0].endsWith('/bpay_next_source_legacy_finance_admit_v1')) return new Response('true');
      return new Response(responseText, { status: 200 });
    },
  });

  assert.equal(await rpc('pay_c1_test', requestText, OPTIONS), responseText);
  assert.equal(calls.length, 2);
  assert.equal(calls[1][0], 'https://test-postgrest.invalid/rest/v1/rpc/pay_c1_test');
  assert.equal(calls[1][1].body, requestText);
  assert.equal(calls[1][1].method, 'POST');
});

test('a returned HTTP refusal is known and is not converted to an ambiguous outcome', async () => {
  const rpc = createWeeklySourceC1RawRpc({
    baseUrl: 'https://test-postgrest.invalid',
    headers: { authorization: 'Bearer test-only' },
    fetchImpl: async (url) => url.endsWith('/bpay_next_source_legacy_finance_admit_v1')
      ? new Response('true') : new Response('{"code":"23514"}', { status: 409 }),
  });
  await assert.rejects(
    () => rpc('pay_c1_test', '{}', OPTIONS),
    (error) => error.status === 409 && error.outcomeKnown === true && error.unknownOutcome === false,
  );
});

test('a lost transport is ambiguous and is never retried', async () => {
  let calls = 0;
  const rpc = createWeeklySourceC1RawRpc({
    baseUrl: 'https://test-postgrest.invalid',
    headers: { authorization: 'Bearer test-only' },
    fetchImpl: async (url) => {
      if (url.endsWith('/bpay_next_source_legacy_finance_admit_v1')) return new Response('true');
      calls += 1;
      throw new Error('connection lost');
    },
  });
  await assert.rejects(
    () => rpc('pay_c1_test', '{}', OPTIONS),
    (error) => error.code === 'C1_RAW_RPC_TRANSPORT_AMBIGUOUS'
      && error.unknownOutcome === true,
  );
  assert.equal(calls, 1);
});

test('refuses any caller that does not request the exact lossless no-retry contract', async () => {
  const rpc = createWeeklySourceC1RawRpc({
    baseUrl: 'https://test-postgrest.invalid',
    headers: { authorization: 'Bearer test-only' },
    fetchImpl: async () => assert.fail('invalid contracts must fail before transport'),
  });
  await assert.rejects(
    () => rpc('pay_c1_test', '{}', { automaticRetry: true }),
    (error) => error.code === 'C1_RAW_RPC_INPUT_INVALID',
  );
});

test('published transport contract contains no Banking Pay or Draft capability', () => {
  assert.equal(WEEKLY_SOURCE_C1_RAW_RPC_CONTRACT.automaticRetry, false);
  assert.equal(WEEKLY_SOURCE_C1_RAW_RPC_CONTRACT.parsedJson, false);
  assert.equal(JSON.stringify(WEEKLY_SOURCE_C1_RAW_RPC_CONTRACT).includes('Draft'), false);
  assert.equal(JSON.stringify(WEEKLY_SOURCE_C1_RAW_RPC_CONTRACT).includes('Banking'), false);
});

// Finite controls for this actual entry boundary; no database data or external action.
test('DB retired refusal is known and prevents the original C1 call', async () => {
  const calls=[];const rpc=createWeeklySourceC1RawRpc({baseUrl:'https://test-postgrest.invalid',headers:{},
    fetchImpl:async(url)=>{calls.push(url);return new Response(JSON.stringify({code:'PT410',message:'BPAY_NEXT_LEGACY_WRITER_RETIRED'}),{status:410});}});
  await assert.rejects(()=>rpc('weekly_source_start_c1','{}',OPTIONS),error=>error.code==='BPAY_NEXT_LEGACY_WRITER_RETIRED'&&error.status===410&&error.outcomeKnown===true&&error.unknownOutcome===false);
  assert.equal(calls.length,1);assert.match(calls[0],/bpay_next_source_legacy_finance_admit_v1$/);
});
test('loss during admission has a known non-attempt, without a C1 recovery call',async()=>{
  let calls=0;const rpc=createWeeklySourceC1RawRpc({baseUrl:'https://test-postgrest.invalid',headers:{},fetchImpl:async()=>{calls++;throw Error('lost');}});
  await assert.rejects(()=>rpc('weekly_source_start_c1','{}',OPTIONS),error=>error.code==='BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'&&error.outcomeKnown===true&&error.unknownOutcome===false&&!error.recoveryCall);
  assert.equal(calls,1);
});
