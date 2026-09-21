import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createExternalEffectFakes,
  UnexpectedExternalNetworkError,
  withExternalNetworkDenied
} from './external-effect-fakes.mjs';

test('TH-017 captures declared email, push and provider outcomes without delivery', async () => {
  const fakes = createExternalEffectFakes({
    emailOutcomes: [{ outcome: 'ACCEPTED', providerReference: 'test-email-1' }],
    pushOutcomes: ['RETRYABLE'],
    providerOutcomes: ['FINAL']
  });
  assert.deepEqual(await fakes.email.send({ messageType: 'MANAGER_QUERY', membership: ['shift_1'] }), {
    outcome: 'ACCEPTED', providerReference: 'test-email-1'
  });
  assert.deepEqual(await fakes.push.send({ messageType: 'CANDIDATE_REMINDER', deviceOrdinal: 1 }), { outcome: 'RETRYABLE' });
  assert.deepEqual(await fakes.provider.send({ operation: 'BOUNDARY_PROBE', requestDigest: 'a'.repeat(64) }), { outcome: 'FINAL' });
  assert.equal(fakes.email.calls()[0].request.membership[0], 'shift_1');
  assert.deepEqual(
    { email: fakes.summary().email.count, push: fakes.summary().push.count, provider: fakes.summary().provider.count },
    { email: 1, push: 1, provider: 1 }
  );
  fakes.assertAllDeclaredOutcomesUsed();
});

test('TH-017 refuses undeclared extra delivery calls', async () => {
  const fakes = createExternalEffectFakes({ emailOutcomes: [] });
  await assert.rejects(() => fakes.email.send({ messageType: 'UNDECLARED' }), /No declared email fake outcome/);
});

test('TH-017 R2 fake retains bytes in memory but reports only bounded hashes', async () => {
  const fakes = createExternalEffectFakes();
  const stored = await fakes.r2.put('weekly-source/test.xlsx', Buffer.from('restricted test workbook bytes'), { contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
  assert.equal(stored.size, 30);
  assert.match(stored.sha256, /^[a-f0-9]{64}$/);
  const fetched = await fakes.r2.get('weekly-source/test.xlsx');
  assert.equal(fetched.bytes.toString('utf8'), 'restricted test workbook bytes');
  await fakes.r2.delete('weekly-source/test.xlsx');
  assert.equal(await fakes.r2.get('weekly-source/test.xlsx'), null);
  const summaryText = JSON.stringify(fakes.r2.summary());
  assert(!summaryText.includes('restricted test workbook bytes'));
});

test('TH-017 unexpected network is blocked and the original fetch is restored', async () => {
  const originalFetch = globalThis.fetch;
  await withExternalNetworkDenied(async ({ networkAttempts }) => {
    await assert.rejects(() => fetch('https://example.invalid'), UnexpectedExternalNetworkError);
    assert.equal(networkAttempts(), 1);
  });
  assert.equal(globalThis.fetch, originalFetch);
});

test('TH-017 permits only an explicitly named local PostgreSQL transport origin', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input) => {
    calls.push(String(input instanceof Request ? input.url : input));
    return new Response('{"ok":true}', { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    await withExternalNetworkDenied(async ({ networkAttempts }) => {
      const response = await fetch('http://127.0.0.1:54321/rest/v1/rpc/example');
      assert.equal(response.status, 200);
      await assert.rejects(() => fetch('http://127.0.0.1:54322/rest/v1/rpc/example'), UnexpectedExternalNetworkError);
      await assert.rejects(() => fetch('https://example.invalid'), UnexpectedExternalNetworkError);
      assert.equal(networkAttempts(), 2);
    }, { allowedLoopbackOrigins: ['http://127.0.0.1:54321'] });
    assert.deepEqual(calls, ['http://127.0.0.1:54321/rest/v1/rpc/example']);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
