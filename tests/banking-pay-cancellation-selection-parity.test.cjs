const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const statusPage = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1146_pay_batch_payment_status_page_v1.sql'
), 'utf8');
const selectionPrepare = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql'
), 'utf8');

function canonicalProviderState(fixture) {
  if (fixture.paidOrSettled === true) return 'FINAL_PAID';
  if (fixture.providerOutcomeUnknown === true) return 'PROVIDER_OUTCOME_UNKNOWN';
  if (fixture.terminalNoMoney === true) return 'TERMINAL_NO_MONEY';
  if (fixture.providerPendingNonFinal === true) return 'PENDING_NON_FINAL';
  return 'NO_TRANSFER_EVIDENCE';
}

function eligibleForFailedRelease(fixture) {
  return canonicalProviderState(fixture) === 'TERMINAL_NO_MONEY'
    && fixture.providerSubmissionInProgress !== true
    && !['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE'].includes(fixture.latestWorkStatus)
    && fixture.manualCarryForwardBlocked !== true
    && fixture.carryForwardFreshnessBlocked !== true;
}

function diagnosticCanNoMoneyUnwind(fixture) {
  const providerPending = fixture.providerPendingNonFinal === true
    && fixture.terminalNoMoney !== true
    && fixture.paidOrSettled !== true;
  return fixture.terminalNoMoney === true
    && fixture.paidOrSettled !== true
    && fixture.providerOutcomeUnknown !== true
    && providerPending !== true
    && fixture.providerSubmissionInProgress !== true
    && !['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE'].includes(fixture.latestWorkStatus)
    && fixture.manualCarryForwardBlocked !== true
    && fixture.carryForwardFreshnessBlocked !== true;
}

test('Current Payment Status has one set-wise failed-release eligibility authority', () => {
  assert.match(statusPage, /candidate_provider_precedence_index AS MATERIALIZED/);
  assert.match(statusPage, /AS canonical_provider_state/);
  assert.match(statusPage, /AS provider_outcome_unknown/);
  assert.match(statusPage, /AS provider_pending_non_final/);
  assert.match(statusPage, /candidate_release_eligibility_index AS MATERIALIZED/);
  assert.match(statusPage, /AS release_failed_payment_eligible/);
  assert.match(statusPage, /canonical_provider_state = 'TERMINAL_NO_MONEY'/);
  assert.match(statusPage, /manual_carry_forward_blocked IS NOT TRUE/);
  assert.match(statusPage, /carry_forward_freshness_blocked IS NOT TRUE/);
  assert.match(statusPage, /provider_submission_in_progress IS NOT TRUE/);
  assert.match(statusPage, /provider_outcome_unknown IS NOT TRUE/);
  assert.doesNotMatch(statusPage, /pay_payment_cancelability_diagnostic\s*\(/);

  const classifiedStart = statusPage.indexOf('candidate_classified_index AS MATERIALIZED');
  const classifiedEnd = statusPage.indexOf('candidate_filtered_index AS MATERIALIZED', classifiedStart);
  const classified = statusPage.slice(classifiedStart, classifiedEnd);
  assert.match(classified, /WHEN release_failed_payment_eligible THEN 'NOT_PAID'/);
  assert.match(classified, /WHEN release_failed_payment_eligible THEN ARRAY\['RELEASE_FAILED_PAYMENT'\]/);
  assert.doesNotMatch(classified, /WHEN terminal_no_money THEN ARRAY\['RELEASE_FAILED_PAYMENT'\]/);
});

test('page rows, filters and selectable actions consume the same eligibility value', () => {
  assert.match(statusPage, /'is_not_paid', page_rows\.release_failed_payment_eligible/);
  assert.match(statusPage, /'release_failed_payment_eligible', page_rows\.release_failed_payment_eligible/);
  assert.match(statusPage, /JOIN candidate_classified_index AS status_index/);
  assert.match(statusPage, /status_index\.release_failed_payment_eligible/);
  assert.match(statusPage, /pg_catalog\.upper\(pg_catalog\.btrim\(v_filter ->> 'action'\)\)[\s\S]*ANY\(classified\.available_actions\)/);
  assert.match(statusPage, /actionable_only[\s\S]*pg_catalog\.cardinality\(classified\.available_actions\) > 0/);
});

test('immutable preparation cannot reinstate blocked or failed failed-release work', () => {
  const actionStart = selectionPrepare.indexOf('v_action_allowed := CASE');
  const actionEnd = selectionPrepare.indexOf('v_effective_display_state := CASE', actionStart);
  const action = selectionPrepare.slice(actionStart, actionEnd);
  assert.match(action, /can_no_money_unwind/);
  for (const status of ['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE']) {
    assert.match(action, new RegExp(`latest_work_status IS DISTINCT FROM '${status}'`));
  }
});

test('failed-release safety matrix is closed for every reviewed blocker family', () => {
  const safe = {
    terminalNoMoney: true,
    paidOrSettled: false,
    providerPendingNonFinal: false,
    providerSubmissionInProgress: false,
    providerOutcomeUnknown: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };
  assert.equal(eligibleForFailedRelease(safe), true);

  for (const unsafe of [
    { paidOrSettled: true },
    { providerSubmissionInProgress: true },
    { providerOutcomeUnknown: true },
    { latestWorkStatus: 'BLOCKED' },
    { latestWorkStatus: 'FAILED_FINAL' },
    { latestWorkStatus: 'FAILED_RETRYABLE' },
    { manualCarryForwardBlocked: true },
    { carryForwardFreshnessBlocked: true },
    { terminalNoMoney: false },
  ]) {
    assert.equal(eligibleForFailedRelease({ ...safe, ...unsafe }), false, JSON.stringify(unsafe));
  }
});

test('terminal no-money outranks historical pending while unknown and paid remain blockers', () => {
  const base = {
    paidOrSettled: false,
    terminalNoMoney: false,
    providerPendingNonFinal: false,
    providerOutcomeUnknown: false,
    providerSubmissionInProgress: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };

  assert.equal(canonicalProviderState({
    ...base,
    terminalNoMoney: true,
    providerPendingNonFinal: true,
  }), 'TERMINAL_NO_MONEY');
  assert.equal(eligibleForFailedRelease({
    ...base,
    terminalNoMoney: true,
    providerPendingNonFinal: true,
  }), true);
  assert.equal(canonicalProviderState({
    ...base,
    terminalNoMoney: true,
    providerOutcomeUnknown: true,
  }), 'PROVIDER_OUTCOME_UNKNOWN');
  assert.equal(canonicalProviderState({
    ...base,
    paidOrSettled: true,
    terminalNoMoney: true,
    providerPendingNonFinal: true,
  }), 'FINAL_PAID');
});

test('reviewed and immutable failed-release sets are equal in both directions', () => {
  const common = {
    paidOrSettled: false,
    terminalNoMoney: false,
    providerPendingNonFinal: false,
    providerOutcomeUnknown: false,
    providerSubmissionInProgress: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };
  const fixtures = [
    { id: 'plain-terminal', ...common, terminalNoMoney: true },
    { id: 'terminal-plus-pending', ...common, terminalNoMoney: true, providerPendingNonFinal: true },
    { id: 'terminal-plus-unknown', ...common, terminalNoMoney: true, providerOutcomeUnknown: true },
    { id: 'terminal-plus-submission', ...common, terminalNoMoney: true, providerSubmissionInProgress: true },
    { id: 'terminal-plus-paid', ...common, terminalNoMoney: true, paidOrSettled: true },
    { id: 'pending-only', ...common, providerPendingNonFinal: true },
    { id: 'unknown-only', ...common, providerOutcomeUnknown: true },
    { id: 'blocked-work', ...common, terminalNoMoney: true, latestWorkStatus: 'BLOCKED' },
    { id: 'failed-final-work', ...common, terminalNoMoney: true, latestWorkStatus: 'FAILED_FINAL' },
    { id: 'failed-retryable-work', ...common, terminalNoMoney: true, latestWorkStatus: 'FAILED_RETRYABLE' },
    { id: 'manual-blocker', ...common, terminalNoMoney: true, manualCarryForwardBlocked: true },
    { id: 'freshness-blocker', ...common, terminalNoMoney: true, carryForwardFreshnessBlocked: true },
    { id: 'active', ...common },
  ];

  const reviewedIds = fixtures.filter(eligibleForFailedRelease).map(({ id }) => id).sort();
  const preparedIds = fixtures.filter(diagnosticCanNoMoneyUnwind).map(({ id }) => id).sort();
  assert.deepEqual(reviewedIds.filter((id) => !preparedIds.includes(id)), []);
  assert.deepEqual(preparedIds.filter((id) => !reviewedIds.includes(id)), []);
  assert.deepEqual(reviewedIds, ['plain-terminal', 'terminal-plus-pending']);
});
