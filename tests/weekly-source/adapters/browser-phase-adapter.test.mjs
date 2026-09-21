import test from 'node:test';
import assert from 'node:assert/strict';

import {
  browserProjectionDigests,
  browserUiStateIds,
  browserUiStatesNotRendered,
  parsePlaywrightSummary,
} from './browser-phase-adapter.mjs';

test('browser adapter accepts only the sealed 31-test passing suite', () => {
  assert.deepEqual(parsePlaywrightSummary('\n  39 passed (9.0s)\n'), { passed: 39, duration: '9.0s' });
});

test('browser adapter refuses missing, duplicated and partial summaries', () => {
  assert.throws(() => parsePlaywrightSummary('30 passed (8.0s)'), { code: 'WEEKLY_SOURCE_BROWSER_COUNT_INVALID' });
  assert.throws(() => parsePlaywrightSummary('39 passed (8.0s)\n39 passed (9.0s)\n'), { code: 'WEEKLY_SOURCE_BROWSER_SUMMARY_INVALID' });
  assert.throws(() => parsePlaywrightSummary('all good'), { code: 'WEEKLY_SOURCE_BROWSER_SUMMARY_INVALID' });
});

test('the controlling browser gate renders all 22 lifecycle states', () => {
  assert.deepEqual(browserUiStateIds(), Array.from({ length: 22 }, (_, index) =>
    `UI-${String(index + 1).padStart(3, '0')}`));
  assert.deepEqual(browserUiStatesNotRendered(), []);
});

test('browser evidence names every lower-case SHA-256 projection digest', () => {
  assert.deepEqual(browserProjectionDigests([{
    spec: 'tests/e2e/example.spec.ts',
    sha256: 'a'.repeat(64),
  }]), [{
    name: 'tests/e2e/example.spec.ts',
    digest: 'a'.repeat(64),
  }]);
});
