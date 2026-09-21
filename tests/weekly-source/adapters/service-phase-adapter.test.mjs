import test from 'node:test';
import assert from 'node:assert/strict';

import { parseMytmsFocusedSummary, parseNodeTestSummary } from './service-phase-adapter.mjs';

test('service adapter accepts only the complete no-skip service proof', () => {
  assert.deepEqual(
    parseNodeTestSummary('ℹ tests 583\nℹ pass 583\nℹ fail 0\nℹ skipped 0\n'),
    { tests: 583, pass: 583, fail: 0, skipped: 0 },
  );
});

test('service adapter refuses partial, failing or skipped evidence', () => {
  assert.throws(() => parseNodeTestSummary('ℹ tests 583\nℹ pass 582\nℹ fail 1\nℹ skipped 0\n'), { code: 'WEEKLY_SOURCE_SERVICE_COUNT_INVALID' });
  assert.throws(() => parseNodeTestSummary('ℹ tests 583\nℹ pass 581\nℹ fail 0\nℹ skipped 2\n'), { code: 'WEEKLY_SOURCE_SERVICE_COUNT_INVALID' });
  assert.throws(() => parseNodeTestSummary(''), { code: 'WEEKLY_SOURCE_SERVICE_COUNT_INVALID' });
});

test('service adapter seals exact Candidate, manager and contract counts', () => {
  assert.deepEqual(parseMytmsFocusedSummary({
    candidateOutput: 'Tests:       108 passed, 108 total',
    managerOutput: 'Tests  20 passed (20)',
    contractOutput: JSON.stringify({ status: 'PASS', operation_count: 68, path_count: 67 }),
  }), {
    candidateTests: 108,
    managerTests: 20,
    operationCount: 68,
    pathCount: 67,
  });
  assert.throws(() => parseMytmsFocusedSummary({
    candidateOutput: 'Tests:       100 passed, 100 total',
    managerOutput: 'Tests  20 passed (20)',
    contractOutput: JSON.stringify({ status: 'PASS', operation_count: 68, path_count: 67 }),
  }), { code: 'WEEKLY_SOURCE_MYTMS_COUNT_INVALID' });
});
