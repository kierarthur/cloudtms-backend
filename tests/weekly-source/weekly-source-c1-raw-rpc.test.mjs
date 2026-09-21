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
      return new Response(responseText, { status: 200 });
    },
  });

  assert.equal(await rpc('pay_c1_test', requestText, OPTIONS), responseText);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], 'https://test-postgrest.invalid/rest/v1/rpc/pay_c1_test');
  assert.equal(calls[0][1].body, requestText);
  assert.equal(calls[0][1].method, 'POST');
});

test('a returned HTTP refusal is known and is not converted to an ambiguous outcome', async () => {
  const rpc = createWeeklySourceC1RawRpc({
    baseUrl: 'https://test-postgrest.invalid',
    headers: { authorization: 'Bearer test-only' },
    fetchImpl: async () => new Response('{"code":"23514"}', { status: 409 }),
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
    fetchImpl: async () => {
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
