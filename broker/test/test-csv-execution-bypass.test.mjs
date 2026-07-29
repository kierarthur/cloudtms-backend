import assert from 'node:assert/strict';
import test from 'node:test';

import {
  evaluateTestCsvExecutionBypass,
  evaluateTestSameWeekPayeOverrideBypass,
  isTestSameWeekPayeOverrideOnlyToken,
  testCsvExecutionTokenMatchesMode
} from '../src/test-csv-execution-bypass.js';

const userId = '11111111-1111-4111-8111-111111111111';
const testEnv = {
  SUPABASE_URL: 'https://yakevhtttcsljosbdpov.supabase.co',
  ALLOWED_ORIGINS: 'https://testmode.arthur-rai.co.uk,https://kierarthur.github.io',
  TEST_CSV_EXECUTION_2FA_BYPASS_ENABLED: 'ENABLED',
  TEST_CSV_EXECUTION_2FA_BYPASS_USER_ID: userId
};

test('allows only the configured TEST user and CSV settlement action', () => {
  assert.equal(evaluateTestCsvExecutionBypass({
    env: testEnv,
    user: { id: userId },
    purpose: 'PAYMENT_SCHEDULE',
    executionMode: 'CSV_SETTLEMENT'
  }).allowed, true);
});

test('allows only the configured TEST user to bypass same-week PAYE draft 2FA after password verification', () => {
  assert.equal(evaluateTestSameWeekPayeOverrideBypass({
    env: testEnv,
    user: { id: userId },
    purpose: 'PAYE_SAME_WEEK_OVERRIDE'
  }).allowed, true);
});

for (const [name, patch] of [
  ['disabled configuration', { env: { ...testEnv, TEST_CSV_EXECUTION_2FA_BYPASS_ENABLED: '' } }],
  ['production database', { env: { ...testEnv, SUPABASE_URL: 'https://zojgukxyyklcyjbyyqlz.supabase.co' } }],
  ['production origin', { env: { ...testEnv, ALLOWED_ORIGINS: 'https://cloudtms.arthur-rai.co.uk' } }],
  ['different user', { user: { id: '22222222-2222-4222-8222-222222222222' } }],
  ['different purpose', { purpose: 'PAYMENT_SCHEDULE' }]
]) {
  test(`same-week PAYE bypass denies ${name}`, () => {
    const input = {
      env: testEnv,
      user: { id: userId },
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
  ['different user', { user: { id: '22222222-2222-4222-8222-222222222222' } }],
  ['different purpose', { purpose: 'PAYMENT_REVERSAL' }],
  ['standard bank execution', { executionMode: 'STANDARD_BANK' }],
  ['external settlement', { executionMode: 'EXTERNAL_SETTLEMENT' }]
]) {
  test(`denies ${name}`, () => {
    const input = {
      env: testEnv,
      user: { id: userId },
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
