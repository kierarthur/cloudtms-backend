const TEST_SUPABASE_URL = 'https://yakevhtttcsljosbdpov.supabase.co';
const TEST_FRONTEND_ORIGIN = 'https://testmode.arthur-rai.co.uk';
const TEST_FUTURE_STANDARD_PAYMENT_AT_UTC = '2026-08-15T01:00:00.000Z';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const text = (value) => String(value == null ? '' : value).trim();
const upper = (value) => text(value).toUpperCase();

function evaluateConfiguredTestUser({ env, user } = {}) {
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

  return enabled
    && isTestProject
    && isTestOriginSet
    && isConfiguredUser;
}

export function evaluateTestCsvExecutionBypass({ env, user, purpose, executionMode } = {}) {
  const isPaymentSchedule = upper(purpose) === 'PAYMENT_SCHEDULE';
  const isCsvSettlement = upper(executionMode) === 'CSV_SETTLEMENT';

  const allowed = evaluateConfiguredTestUser({ env, user })
    && isPaymentSchedule
    && isCsvSettlement;

  return {
    allowed,
    reason: allowed
      ? 'TEST_CSV_SETTLEMENT_BYPASS_ALLOWED'
      : 'TEST_CSV_SETTLEMENT_BYPASS_DENIED'
  };
}

export function evaluateTestSameWeekPayeOverrideBypass({ env, user, purpose } = {}) {
  const allowed = evaluateConfiguredTestUser({ env, user })
    && upper(purpose) === 'PAYE_SAME_WEEK_OVERRIDE';

  return {
    allowed,
    reason: allowed
      ? 'TEST_SAME_WEEK_PAYE_OVERRIDE_BYPASS_ALLOWED'
      : 'TEST_SAME_WEEK_PAYE_OVERRIDE_BYPASS_DENIED'
  };
}

const canonicalIso = (value) => {
  const parsed = Date.parse(text(value));
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : '';
};

export function evaluateTestFutureStandardPaymentBypass({
  env,
  user,
  purpose,
  executionMode,
  scheduleKind,
  scheduledAtUtc
} = {}) {
  const allowed = evaluateConfiguredTestUser({ env, user })
    && upper(purpose) === 'PAYMENT_SCHEDULE'
    && upper(executionMode) === 'STANDARD_BANK'
    && upper(scheduleKind) === 'SCHEDULED'
    && canonicalIso(scheduledAtUtc) === TEST_FUTURE_STANDARD_PAYMENT_AT_UTC;

  return {
    allowed,
    scheduledAtUtc: allowed ? TEST_FUTURE_STANDARD_PAYMENT_AT_UTC : null,
    reason: allowed
      ? 'TEST_FUTURE_STANDARD_PAYMENT_BYPASS_ALLOWED'
      : 'TEST_FUTURE_STANDARD_PAYMENT_BYPASS_DENIED'
  };
}

export function evaluateTestPaymentReversalBypass({ env, user, purpose } = {}) {
  const allowed = evaluateConfiguredTestUser({ env, user })
    && upper(purpose) === 'PAYMENT_REVERSAL';

  return {
    allowed,
    reason: allowed
      ? 'TEST_PAYMENT_REVERSAL_BYPASS_ALLOWED'
      : 'TEST_PAYMENT_REVERSAL_BYPASS_DENIED'
  };
}

export function isTestCsvExecutionOnlyToken(payload) {
  return payload?.test_csv_execution_only === true;
}

export function isTestSameWeekPayeOverrideOnlyToken(payload) {
  return payload?.test_same_week_paye_override_only === true;
}

export function isTestFutureStandardPaymentOnlyToken(payload) {
  return payload?.test_future_standard_payment_only === true;
}

export function isTestPaymentReversalOnlyToken(payload) {
  return payload?.test_payment_reversal_only === true;
}

export function testCsvExecutionTokenMatchesMode(payload, executionMode) {
  if (!isTestCsvExecutionOnlyToken(payload)) return true;
  return upper(executionMode) === 'CSV_SETTLEMENT';
}

export function testPaymentScheduleTokenMatchesRequest(payload, request = {}) {
  if (!testCsvExecutionTokenMatchesMode(payload, request.executionMode)) return false;
  if (!isTestFutureStandardPaymentOnlyToken(payload)) return true;

  return upper(request.executionMode) === 'STANDARD_BANK'
    && upper(request.scheduleKind) === 'SCHEDULED'
    && canonicalIso(request.scheduledAtUtc) === TEST_FUTURE_STANDARD_PAYMENT_AT_UTC
    && canonicalIso(payload?.test_scheduled_at_utc) === TEST_FUTURE_STANDARD_PAYMENT_AT_UTC;
}
