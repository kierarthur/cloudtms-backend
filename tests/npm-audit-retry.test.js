import assert from 'node:assert/strict';
import test from 'node:test';

import {
  AUDIT_FETCH_TIMEOUT_MS,
  MAX_AUDIT_ATTEMPTS,
  auditArguments,
  classifyAuditResult,
  runNpmAuditWithRetry
} from '../scripts/run-npm-audit.mjs';

test('npm audit arguments retain the high-severity gate and bound transport waiting', () => {
  assert.deepEqual(auditArguments(), [
    'audit',
    '--audit-level=high',
    '--fetch-retries=0',
    `--fetch-timeout=${AUDIT_FETCH_TIMEOUT_MS}`
  ]);
  assert.deepEqual(auditArguments({ omitDev: true }), [
    'audit',
    '--omit=dev',
    '--audit-level=high',
    '--fetch-retries=0',
    `--fetch-timeout=${AUDIT_FETCH_TIMEOUT_MS}`
  ]);
});

test('only npm audit service and transport failures are retryable', () => {
  assert.equal(classifyAuditResult({ status: 0 }), 'PASS');
  assert.equal(classifyAuditResult({ status: 1, stderr: 'npm warn audit 503 Service Unavailable\nnpm error audit endpoint returned an error' }), 'TRANSIENT_SERVICE_FAILURE');
  assert.equal(classifyAuditResult({ status: 1, stderr: 'npm error code ETIMEDOUT' }), 'TRANSIENT_SERVICE_FAILURE');
  assert.equal(classifyAuditResult({ status: 1, stdout: 'high severity vulnerability found' }), 'SECURITY_FAILURE');
  assert.equal(classifyAuditResult({ status: 1, stderr: 'package-lock.json is invalid' }), 'SECURITY_FAILURE');
});

test('a vulnerability failure is returned immediately and is never retried', async () => {
  let calls = 0;
  const status = await runNpmAuditWithRetry({
    execute: () => { calls += 1; return { status: 1, stdout: 'high severity vulnerability found', stderr: '' }; },
    delay: async () => assert.fail('security failures must not be retried'),
    writeStdout: () => undefined,
    writeStderr: () => undefined
  });
  assert.equal(status, 1);
  assert.equal(calls, 1);
});

test('a transient npm service failure retries and can recover', async () => {
  let calls = 0;
  const delays = [];
  const status = await runNpmAuditWithRetry({
    execute: () => {
      calls += 1;
      return calls < 3
        ? { status: 1, stdout: '', stderr: 'npm error audit endpoint returned an error' }
        : { status: 0, stdout: 'found 0 vulnerabilities', stderr: '' };
    },
    delay: async (value) => { delays.push(value); },
    writeStdout: () => undefined,
    writeStderr: () => undefined
  });
  assert.equal(status, 0);
  assert.equal(calls, MAX_AUDIT_ATTEMPTS);
  assert.deepEqual(delays, [15_000, 30_000]);
});

test('persistent npm service failure exhausts the bound and fails closed', async () => {
  let calls = 0;
  const status = await runNpmAuditWithRetry({
    execute: () => { calls += 1; return { status: 1, stdout: '', stderr: 'npm error code ECONNRESET' }; },
    delay: async () => undefined,
    writeStdout: () => undefined,
    writeStderr: () => undefined
  });
  assert.equal(status, 1);
  assert.equal(calls, MAX_AUDIT_ATTEMPTS);
});
