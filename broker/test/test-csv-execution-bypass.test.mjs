import assert from 'node:assert/strict';
import test from 'node:test';

import {
  evaluateTestCsvExecutionBypass,
  evaluateTestFutureStandardPaymentBypass,
  evaluateTestPaymentReversalBypass,
  evaluateTestSameWeekPayeOverrideBypass,
  isTestFutureStandardPaymentOnlyToken,
  isTestPaymentReversalOnlyToken,
  isTestSameWeekPayeOverrideOnlyToken,
  testCsvExecutionTokenMatchesMode,
  testPaymentScheduleTokenMatchesRequest
} from '../src/test-csv-execution-bypass.js';

const userId = '11111111-1111-4111-8111-111111111111';
const testUser = { id: userId, email: 'test@arthur-rai.co.uk' };
const testEnv = {
  SUPABASE_URL: 'https://codex-cloudtms-miget-gateway.kier-88a.workers.dev',
  ALLOWED_ORIGINS: 'https://testmode.arthur-rai.co.uk,https://kierarthur.github.io',
  TEST_CSV_EXECUTION_2FA_BYPASS_ENABLED: 'ENABLED',
  TEST_CSV_EXECUTION_2FA_BYPASS_USER_ID: userId
};

test('allows only the exact TEST email and CSV settlement action', () => {
  assert.equal(evaluateTestCsvExecutionBypass({
    env: testEnv,
    user: testUser,
    purpose: 'PAYMENT_SCHEDULE',
    executionMode: 'CSV_SETTLEMENT'
  }).allowed, true);
});

test('allows only the exact TEST email to bypass same-week PAYE draft 2FA after password verification', () => {
  assert.equal(evaluateTestSameWeekPayeOverrideBypass({
    env: testEnv,
    user: testUser,
    purpose: 'PAYE_SAME_WEEK_OVERRIDE'
  }).allowed, true);
});

test('allows only the exact TEST email for a valid scheduled STANDARD_BANK test payment', () => {
  const result = evaluateTestFutureStandardPaymentBypass({
    env: testEnv,
    user: testUser,
    purpose: 'PAYMENT_SCHEDULE',
    executionMode: 'STANDARD_BANK',
    scheduleKind: 'SCHEDULED',
    scheduledAtUtc: '2026-08-15T02:00:00+01:00'
  });
  assert.equal(result.allowed, true);
  assert.equal(result.scheduledAtUtc, '2026-08-15T01:00:00.000Z');
});

test('the exact TEST email remains the authority even when a stale configured user ID is present', () => {
  assert.equal(evaluateTestPaymentReversalBypass({
    env: { ...testEnv, TEST_CSV_EXECUTION_2FA_BYPASS_USER_ID: '22222222-2222-4222-8222-222222222222' },
    user: testUser,
    purpose: 'PAYMENT_REVERSAL'
  }).allowed, true);
});

test('scheduled STANDARD_BANK bypass binds to the requested valid UTC instant', () => {
  const result = evaluateTestFutureStandardPaymentBypass({
    env: testEnv,
    user: testUser,
    purpose: 'PAYMENT_SCHEDULE',
    executionMode: 'STANDARD_BANK',
    scheduleKind: 'SCHEDULED',
    scheduledAtUtc: '2026-08-22T02:00:00+01:00'
  });
  assert.equal(result.allowed, true);
  assert.equal(result.scheduledAtUtc, '2026-08-22T01:00:00.000Z');
  assert.equal(testPaymentScheduleTokenMatchesRequest({
    test_future_standard_payment_only: true,
    test_scheduled_at_utc: result.scheduledAtUtc
  }, {
    executionMode: 'STANDARD_BANK',
    scheduleKind: 'SCHEDULED',
    scheduledAtUtc: '2026-08-22T02:00:00+01:00'
  }), true);
});

for (const [name, patch] of [
  ['different email', { user: { id: userId, email: 'someone-else@arthur-rai.co.uk' } }],
  ['immediate schedule', { scheduleKind: 'IMMEDIATE' }],
  ['CSV mode', { executionMode: 'CSV_SETTLEMENT' }],
  ['missing date', { scheduledAtUtc: '' }],
  ['reversal purpose', { purpose: 'PAYMENT_REVERSAL' }]
]) {
  test(`future standard-payment bypass denies ${name}`, () => {
    const input = {
      env: testEnv,
      user: testUser,
      purpose: 'PAYMENT_SCHEDULE',
      executionMode: 'STANDARD_BANK',
      scheduleKind: 'SCHEDULED',
      scheduledAtUtc: '2026-08-15T01:00:00.000Z',
      ...patch
    };
    assert.equal(evaluateTestFutureStandardPaymentBypass(input).allowed, false);
  });
}

test('allows only the exact TEST email to bypass payment-reversal 2FA', () => {
  assert.equal(evaluateTestPaymentReversalBypass({
    env: testEnv,
    user: testUser,
    purpose: 'PAYMENT_REVERSAL'
  }).allowed, true);
  assert.equal(evaluateTestPaymentReversalBypass({
    env: testEnv,
    user: testUser,
    purpose: 'PAYMENT_SCHEDULE'
  }).allowed, false);
});

for (const [name, patch] of [
  ['disabled configuration', { env: { ...testEnv, TEST_CSV_EXECUTION_2FA_BYPASS_ENABLED: '' } }],
  ['production database', { env: { ...testEnv, SUPABASE_URL: 'https://zojgukxyyklcyjbyyqlz.supabase.co' } }],
  ['production origin', { env: { ...testEnv, ALLOWED_ORIGINS: 'https://cloudtms.arthur-rai.co.uk' } }],
  ['different email', { user: { id: userId, email: 'someone-else@arthur-rai.co.uk' } }],
  ['different purpose', { purpose: 'PAYMENT_SCHEDULE' }]
]) {
  test(`same-week PAYE bypass denies ${name}`, () => {
    const input = {
      env: testEnv,
      user: testUser,
      purpose: 'PAYE_SAME_WEEK_OVERRIDE',
      ...patch
    };
    assert.equal(evaluateTestSameWeekPayeOverrideBypass(input).allowed, false);
  });
}

test('same-week PAYE TEST token is explicitly purpose restricted', () => {
  assert.equal(isTestSameWeekPayeOverrideOnlyToken({ test_same_week_paye_override_only: true }), true);
  assert.equal(isTestSameWeekPayeOverrideOnlyToken({ test_csv_execution_only: true }), false);
  assert.equal(isTestSameWeekPayeOverrideOnlyToken({}), false);
});

for (const [name, patch] of [
  ['disabled configuration', { env: { ...testEnv, TEST_CSV_EXECUTION_2FA_BYPASS_ENABLED: '' } }],
  ['production database', { env: { ...testEnv, SUPABASE_URL: 'https://zojgukxyyklcyjbyyqlz.supabase.co' } }],
  ['production origin', { env: { ...testEnv, ALLOWED_ORIGINS: 'https://cloudtms.arthur-rai.co.uk' } }],
  ['different email', { user: { id: userId, email: 'someone-else@arthur-rai.co.uk' } }],
  ['different purpose', { purpose: 'PAYMENT_REVERSAL' }],
  ['standard bank execution', { executionMode: 'STANDARD_BANK' }],
  ['external settlement', { executionMode: 'EXTERNAL_SETTLEMENT' }]
]) {
  test(`denies ${name}`, () => {
    const input = {
      env: testEnv,
      user: testUser,
      purpose: 'PAYMENT_SCHEDULE',
      executionMode: 'CSV_SETTLEMENT',
      ...patch
    };
    assert.equal(evaluateTestCsvExecutionBypass(input).allowed, false);
  });
}

test('CSV-only token cannot be reused for another execution mode', () => {
  const payload = { test_csv_execution_only: true };
  assert.equal(testCsvExecutionTokenMatchesMode(payload, 'CSV_SETTLEMENT'), true);
  assert.equal(testCsvExecutionTokenMatchesMode(payload, 'STANDARD_BANK'), false);
  assert.equal(testCsvExecutionTokenMatchesMode(payload, 'EXTERNAL_SETTLEMENT'), false);
});

test('normal reauthentication tokens remain compatible with all modes', () => {
  assert.equal(testCsvExecutionTokenMatchesMode({}, 'STANDARD_BANK'), true);
  assert.equal(testCsvExecutionTokenMatchesMode({}, 'CSV_SETTLEMENT'), true);
});

test('future-payment TEST token is bound to mode, schedule kind and exact UTC instant', () => {
  const payload = {
    test_future_standard_payment_only: true,
    test_scheduled_at_utc: '2026-08-15T01:00:00.000Z'
  };
  assert.equal(isTestFutureStandardPaymentOnlyToken(payload), true);
  assert.equal(testPaymentScheduleTokenMatchesRequest(payload, {
    executionMode: 'STANDARD_BANK',
    scheduleKind: 'SCHEDULED',
    scheduledAtUtc: '2026-08-15T02:00:00+01:00'
  }), true);
  assert.equal(testPaymentScheduleTokenMatchesRequest(payload, {
    executionMode: 'STANDARD_BANK',
    scheduleKind: 'IMMEDIATE',
    scheduledAtUtc: null
  }), false);
  assert.equal(testPaymentScheduleTokenMatchesRequest(payload, {
    executionMode: 'STANDARD_BANK',
    scheduleKind: 'SCHEDULED',
    scheduledAtUtc: '2026-08-15T01:01:00.000Z'
  }), false);
});

test('payment-reversal TEST token is explicitly purpose restricted', () => {
  assert.equal(isTestPaymentReversalOnlyToken({ test_payment_reversal_only: true }), true);
  assert.equal(isTestPaymentReversalOnlyToken({ test_future_standard_payment_only: true }), false);
});
