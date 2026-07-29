import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.resolve(here, '../src/index.js'), 'utf8');

function sliceBetween(start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  assert.notEqual(from, -1, `missing start marker: ${start}`);
  assert.notEqual(to, -1, `missing end marker: ${end}`);
  return source.slice(from, to);
}

test('payee-map setup failure returns a bounded stable error envelope', () => {
  const handler = sliceBetween(
    'async function handleBankingPayEnsurePayeeMap',
    'async function resolveRailPayeeTargetByHash'
  );

  assert.match(handler, /const failureReason = String\(railSetup\?\.reason/);
  assert.match(handler, /RAIL_PROVIDER_SETUP_FAILED/);
  assert.match(handler, /No payment or draft was created/);
  assert.match(handler, /error_code: failureReason/);
  assert.match(handler, /rail_setup: safeRailSetup/);
  assert.match(handler, /\[BANK_PAYEE_MAP_ENSURE\] setup not completed/);
  assert.match(handler, /safeRailSetup = \(railSetup && typeof railSetup === 'object'\)/);
  assert.doesNotMatch(handler, /error: railSetup\?\.error/);
});
