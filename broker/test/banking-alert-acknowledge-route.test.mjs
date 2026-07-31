import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const workerSource = fs.readFileSync(path.resolve(here, '../src/index.js'), 'utf8');
const handlerStart = workerSource.indexOf('async function handleBankingAlertAcknowledge(env, req, user)');
const handlerEnd = workerSource.indexOf('\nasync function ', handlerStart + 1);

assert.notEqual(handlerStart, -1, 'Banking alert acknowledgement handler is missing');
assert.notEqual(handlerEnd, -1, 'Banking alert acknowledgement handler boundary is missing');

const handlerSource = workerSource.slice(handlerStart, handlerEnd);

test('Banking alert acknowledgement retries one transient upstream failure only', () => {
  assert.match(handlerSource, /const runAlertAcknowledgeRpc = async \(rpcName, rpcArgs\) =>/);
  assert.match(handlerSource, /for \(let attempt = 0; attempt < 2; attempt \+= 1\)/);
  assert.match(handlerSource, /const retryable = status >= 500/);
  assert.match(handlerSource, /if \(attempt > 0 \|\| !retryable\) throw error/);
  assert.match(handlerSource, /runAlertAcknowledgeRpc\('banking_alert_acknowledge_many'/);
  assert.match(handlerSource, /runAlertAcknowledgeRpc\('banking_alert_acknowledge_all_current'/);
});

test('Banking alert acknowledgement failure remains user friendly and payment-safe', () => {
  assert.match(handlerSource, /The Banking alert service did not complete the clear request\./);
  assert.match(handlerSource, /No payment status was changed/);
  assert.doesNotMatch(handlerSource, /return withCORS\(env, req, jsonResponse\(500, \{[^}]*stack/);
});

test('Banking alert acknowledgement returns the detailed grouped summary nested by the signal RPC', () => {
  assert.match(handlerSource, /const nestedSummary = safeObject\(signal\.summary_json \|\| signal\.summaryJson\)/);
  assert.match(handlerSource, /return \{[\s\S]*\.\.\.nestedSummary,[\s\S]*banking_unacknowledged_alert_count:[\s\S]*banking_alert_hash:/);
  assert.match(handlerSource, /alert_summary: alertSummary/);
  assert.match(handlerSource, /remaining_alert_summary: alertSummary/);
});
