import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const brokerSource = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const alertKind = 'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED';

function between(source, start, end) {
  const startAt = source.indexOf(start);
  assert.notEqual(startAt, -1, `missing start boundary: ${start}`);
  const endAt = source.indexOf(end, startAt + start.length);
  assert.notEqual(endAt, -1, `missing end boundary: ${end}`);
  return source.slice(startAt, endAt);
}

test('Banking alert preference update accepts the investigation alert kind', () => {
  const handler = between(
    brokerSource,
    'async function handleBankingAlertPreferencesUpdate',
    'async function handleBankingProviderWebhook'
  );
  assert.equal((handler.match(new RegExp(`'${alertKind}'`, 'g')) || []).length, 1);
  assert.match(handler, /const allowedFailureReasonGroups = new Set\(\[[\s\S]*'MANUAL_ADJUSTMENT_BLOCKER'/);
});

test('settings advertise the same investigation alert kind exactly once', () => {
  const handler = between(
    brokerSource,
    'async function handleGetSettings',
    'async function handleUpdateSettings'
  );
  assert.equal((handler.match(new RegExp(`'${alertKind}'`, 'g')) || []).length, 1);
  assert.match(handler, /allowed_alert_kinds:\s*\[[\s\S]*'MANUAL_ADJUSTMENT_BLOCKER'/);
});

test('the investigation kind is confined to the two established Worker allowlists', () => {
  assert.equal((brokerSource.match(new RegExp(`'${alertKind}'`, 'g')) || []).length, 2);
});
