const TEST_SUPABASE_URL = 'https://yakevhtttcsljosbdpov.supabase.co';
const TEST_FRONTEND_ORIGIN = 'https://testmode.arthur-rai.co.uk';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const text = (value) => String(value == null ? '' : value).trim();
const upper = (value) => text(value).toUpperCase();

export function evaluateTestCsvExecutionBypass({ env, user, purpose, executionMode } = {}) {
  const configuredUserId = text(env?.TEST_CSV_EXECUTION_2FA_BYPASS_USER_ID);
  const userId = text(user?.id);
  const enabled = upper(env?.TEST_CSV_EXECUTION_2FA_BYPASS_ENABLED) === 'ENABLED';
  const isTestProject = text(env?.SUPABASE_URL) === TEST_SUPABASE_URL;
  const allowedOrigins = text(env?.ALLOWED_ORIGINS)
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const isTestOriginSet = allowedOrigins.includes(TEST_FRONTEND_ORIGIN)
    && !allowedOrigins.includes('https://cloudtms.arthur-rai.co.uk');
  const isConfiguredUser = UUID_RE.test(configuredUserId) && userId === configuredUserId;
  const isPaymentSchedule = upper(purpose) === 'PAYMENT_SCHEDULE';
  const isCsvSettlement = upper(executionMode) === 'CSV_SETTLEMENT';

  const allowed = enabled
    && isTestProject
    && isTestOriginSet
    && isConfiguredUser
    && isPaymentSchedule
    && isCsvSettlement;

  return {
    allowed,
    reason: allowed
      ? 'TEST_CSV_SETTLEMENT_BYPASS_ALLOWED'
      : 'TEST_CSV_SETTLEMENT_BYPASS_DENIED'
  };
}

export function isTestCsvExecutionOnlyToken(payload) {
  return payload?.test_csv_execution_only === true;
}

export function testCsvExecutionTokenMatchesMode(payload, executionMode) {
  if (!isTestCsvExecutionOnlyToken(payload)) return true;
  return upper(executionMode) === 'CSV_SETTLEMENT';
}
