const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const requestStart = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '04082026_1207_pay_payment_correction_request_start.sql'),
  'utf8',
);
const paymentAuthStart = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '04082026_1158_pay_batch_auth_start.sql'),
  'utf8',
);

test('cancellation authority is exactly one user and is independent of payment execution authoriser quantity', () => {
  const settingsRead = requestStart.slice(
    requestStart.indexOf('SELECT settings_row.banking_pay_candidate_cancellation_enabled'),
    requestStart.indexOf('LIMIT 1;', requestStart.indexOf('SELECT settings_row.banking_pay_candidate_cancellation_enabled')) + 'LIMIT 1;'.length,
  );
  assert.doesNotMatch(settingsRead, /payment_authoriser_quantity/);
  assert.match(requestStart, /v_required_quantity := 1;/);
  assert.match(requestStart, /required_quantity = 1,/);
  assert.match(requestStart, /approved_count = 1,/);

  // Payment creation/execution retains the organisation's independent setting.
  assert.match(paymentAuthStart, /payment_authoriser_quantity/);
});

test('requester reauthentication atomically authorises and starts cancellation work', () => {
  assert.match(requestStart, /SET status = 'AUTHORISED'/);
  assert.match(requestStart, /SET status = 'RUNNING',\s*phase = 'EXPAND_WORK',\s*runner_state = 'RUNNABLE'/);
  assert.match(requestStart, /requires_user_action = false/);
  assert.match(requestStart, /'requires_authorisation', false/);
  assert.match(requestStart, /'code', CASE WHEN v_command = 'START_AUTO' THEN 'AUTO_REQUEST_STARTED' ELSE 'PAYMENT_CORRECTION_AUTHORISED' END/);
  assert.doesNotMatch(requestStart, /SET status = CASE WHEN v_command = 'START_AUTO' THEN 'RUNNING' ELSE 'WAITING_AUTHORISATION'/);
});

test('an already reauthenticated one-user request can be resumed without creating a duplicate', () => {
  assert.match(requestStart, /v_resume_reauthenticated_request :=/);
  assert.match(requestStart, /v_request\.status IN \('REQUESTED', 'AWAITING_AUTHORISATION'\)/);
  assert.match(requestStart, /v_request\.requested_by_user_id IS NOT DISTINCT FROM p_actor_user_id/);
  assert.match(requestStart, /v_request\.reauth_consumed_at_utc IS NOT NULL/);
  assert.match(requestStart, /coalesce\(v_resume_reauthenticated_request, false\) IS NOT TRUE[\s\S]*?v_request\.reauth_expires_at_utc <= v_now/);
  assert.match(requestStart, /LEGACY_REQUESTER_REAUTHORISED_CANCELLATION_RESUMED/);
  assert.match(requestStart, /CASE WHEN coalesce\(v_resume_reauthenticated_request, false\) THEN 'AUTHORISE' ELSE 'REQUEST' END/);
});

test('authorising a cancellation invalidates old payment authority and schedule before work runs', () => {
  const invalidation = requestStart.slice(
    requestStart.indexOf("IF v_command IN ('START_AUTO', 'START_PREPARED') THEN"),
    requestStart.indexOf('UPDATE public.pay_payment_correction_requests AS started_request'),
  );
  assert.match(invalidation, /UPDATE public\.pay_batch_auth_requests AS old_auth/);
  assert.match(invalidation, /SET state = 'CANCELLED'/);
  assert.match(invalidation, /UPDATE public\.pay_batch_auth_tokens AS old_token/);
  assert.match(invalidation, /schedule_kind = NULL/);
  assert.match(invalidation, /scheduled_at_utc = NULL/);
  assert.match(invalidation, /requester_reauthenticated_cancellation/);
});
