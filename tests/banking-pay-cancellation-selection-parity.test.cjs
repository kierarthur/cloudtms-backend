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
const correctionStatus = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1145_pay_payment_correction_status_get_v1.sql'
), 'utf8');
const catalogueFunctions = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '26052026_2100HRS_NEW_FUNCTIONS.sql'
), 'utf8');

function canonicalProviderState(fixture) {
  const paidOrSettled = fixture.paidOrSettled === true || fixture.unscopedFinalPaid === true;
  const terminalNoMoney = fixture.terminalNoMoney === true || fixture.unscopedTerminalNoMoney === true;
  const providerOutcomeUnknown = fixture.providerOutcomeUnknown === true
    || fixture.unscopedProviderOutcomeUnknown === true;
  const providerPendingNonFinal = fixture.providerPendingNonFinal === true
    || fixture.unscopedPendingNonFinal === true;
  const providerOutage = fixture.providerOutage === true || fixture.unscopedProviderOutage === true;
  const providerRequestSent = fixture.providerRequestSent === true
    || fixture.providerEventPresent === true
    || fixture.providerResponsePresent === true
    || fixture.providerExternalIdPresent === true
    || fixture.unscopedProviderRequestSent === true
    || (fixture.operationPayload && operationPayloadHasProviderRequest(fixture.operationPayload));
  const providerExternalIdPresent = fixture.providerExternalIdPresent === true
    || fixture.unscopedProviderExternalIdPresent === true;

  if (paidOrSettled) return 'FINAL_PAID';
  if (providerOutcomeUnknown) return 'PROVIDER_OUTCOME_UNKNOWN';
  const effectiveProviderPending = providerPendingNonFinal
    && (fixture.localPreparedOnly !== true
      || providerRequestSent
      || providerExternalIdPresent
      || fixture.providerSubmissionInProgress === true);

  if ((effectiveProviderPending || providerRequestSent || providerExternalIdPresent
      || fixture.providerSubmissionInProgress === true)
      && !terminalNoMoney && !paidOrSettled) return 'PENDING_NON_FINAL';
  if (providerOutage
      && !providerRequestSent
      && !providerExternalIdPresent
      && fixture.providerSubmissionInProgress !== true) return 'PROVIDER_OUTAGE_RETRY_LATER';
  if (terminalNoMoney) return 'TERMINAL_NO_MONEY';
  return 'NO_TRANSFER_EVIDENCE';
}

function eligibleForFailedRelease(fixture) {
  return canonicalProviderState(fixture) === 'TERMINAL_NO_MONEY'
    && fixture.providerSubmissionInProgress !== true
    && !['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE'].includes(fixture.latestWorkStatus)
    && fixture.manualCarryForwardBlocked !== true
    && fixture.carryForwardFreshnessBlocked !== true
    && fixture.scopeIsFull !== false;
}

function diagnosticCanNoMoneyUnwind(fixture) {
  return canonicalProviderState(fixture) === 'TERMINAL_NO_MONEY'
    && fixture.providerSubmissionInProgress !== true
    && !['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE'].includes(fixture.latestWorkStatus)
    && fixture.manualCarryForwardBlocked !== true
    && fixture.carryForwardFreshnessBlocked !== true
    && fixture.scopeIsFull !== false;
}

function eligibleForOrdinaryCancellation(fixture) {
  return canonicalProviderState(fixture) === 'NO_TRANSFER_EVIDENCE'
    && fixture.activeItemCount !== 0
    && fixture.providerSubmissionInProgress !== true
    && !['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE'].includes(fixture.latestWorkStatus)
    && fixture.manualCarryForwardBlocked !== true
    && fixture.carryForwardFreshnessBlocked !== true
    && fixture.scopeIsFull !== false;
}

function diagnosticCanPreProviderCancel(fixture) {
  const providerEvidencePresent = fixture.providerRequestSent === true
    || fixture.providerExternalIdPresent === true
    || fixture.providerEventPresent === true
    || fixture.providerResponsePresent === true;
  const localNotSent = fixture.paidOrSettled !== true
    && fixture.terminalNoMoney !== true
    && (fixture.providerPendingNonFinal !== true || fixture.localPreparedOnly === true)
    && fixture.providerOutcomeUnknown !== true
    && fixture.providerOutage !== true
    && fixture.unscopedTerminalNoMoney !== true
    && fixture.unscopedPendingNonFinal !== true
    && fixture.unscopedProviderOutcomeUnknown !== true
    && fixture.unscopedProviderOutage !== true
    && fixture.providerSubmissionInProgress !== true
    && !providerEvidencePresent;

  return localNotSent
    && fixture.scopeIsFull !== false
    && fixture.manualCarryForwardBlocked !== true
    && fixture.carryForwardFreshnessBlocked !== true;
}

function preparedOrdinaryCancellationEligible(fixture) {
  return diagnosticCanPreProviderCancel(fixture)
    && !['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE'].includes(fixture.latestWorkStatus);
}

function nonEmptyProviderValue(value) {
  if (value === null || value === undefined) return false;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === 'object') return Object.keys(value).length > 0;
  if (typeof value === 'number') return true;
  if (typeof value !== 'string') return false;
  const normalised = value.trim().toLowerCase();
  return normalised !== '' && !['null', 'false', 'true', 'none', 'n/a', 'na'].includes(normalised);
}

function operationPayloadHasProviderRequest(payload) {
  const truthy = (value) => ['true', 't', 'yes', 'y', '1'].includes(String(value ?? '').trim().toLowerCase());
  const positive = (value) => /^\d+(\.\d+)?$/.test(String(value ?? '').trim()) && Number(value) > 0;
  const diagnostic = payload.provider_submit_diagnostic ?? {};
  const camelDiagnostic = payload.providerSubmitDiagnostic ?? {};
  const providerEvidence = payload.provider_evidence ?? {};
  const camelEvidence = payload.providerEvidence ?? {};
  const diagnosticEnvelope = payload.diagnostic ?? {};
  const outcome = payload.outcome ?? {};
  return [
    payload.provider_request_sent,
    payload.providerRequestSent,
    payload.provider_request_sent_confirmed,
    payload.providerRequestSentConfirmed,
    payload.request_sent,
    payload.requestSent,
    payload.request_dispatched,
    payload.requestDispatched,
    payload.provider_submit_attempted,
    payload.providerSubmitAttempted,
    diagnostic.provider_request_sent,
    camelDiagnostic.providerRequestSent,
    providerEvidence.provider_request_sent,
    camelEvidence.providerRequestSent,
    diagnosticEnvelope.provider_request_sent,
    diagnosticEnvelope.providerRequestSent,
    outcome.provider_request_sent,
    outcome.providerRequestSent,
  ].some(truthy)
    || [
      payload.request_sent_at_utc,
      payload.provider_request_sent_at_utc,
      payload.requestSentAtUtc,
      payload.providerRequestSentAtUtc,
      diagnostic.request_sent_at_utc,
      camelDiagnostic.requestSentAtUtc,
    ].some((value) => nonEmptyProviderValue(value))
    || [
      payload.provider_request_sent_count,
      payload.providerRequestSentCount,
      payload.provider_call_sent_count,
      payload.providerCallSentCount,
      diagnostic.provider_request_sent_count,
      camelDiagnostic.providerRequestSentCount,
    ].some(positive)
    || [
      payload.provider_response,
      payload.provider_response_json,
      payload.submit_response,
      payload.response_json,
      payload.provider_result,
      payload.provider_payload,
    ].some(nonEmptyProviderValue);
}

test('Current Payment Status has one set-wise failed-release eligibility authority', () => {
  assert.match(statusPage, /candidate_provider_precedence_index AS MATERIALIZED/);
  assert.match(statusPage, /AS canonical_provider_state/);
  assert.match(statusPage, /AS provider_outcome_unknown/);
  assert.match(statusPage, /AS provider_pending_non_final/);
  assert.match(statusPage, /AS provider_outage/);
  assert.match(statusPage, /AS provider_request_sent/);
  assert.match(statusPage, /AS provider_external_id_present/);
  assert.match(statusPage, /batch_unscoped_event_facts AS MATERIALIZED/);
  assert.match(statusPage, /unscoped_event\.pay_bank_transfer_id IS NULL/);
  assert.match(statusPage, /THEN 'PROVIDER_OUTAGE_RETRY_LATER'/);
  assert.match(statusPage, /candidate_release_eligibility_index AS MATERIALIZED/);
  assert.match(statusPage, /AS release_failed_payment_eligible/);
  assert.match(statusPage, /canonical_provider_state = 'TERMINAL_NO_MONEY'/);
  assert.match(statusPage, /manual_carry_forward_blocked IS NOT TRUE/);
  assert.match(statusPage, /carry_forward_freshness_blocked IS NOT TRUE/);
  assert.match(statusPage, /provider_submission_in_progress IS NOT TRUE/);
  assert.match(statusPage, /provider_outcome_unknown IS NOT TRUE/);
  assert.match(statusPage, /AS complete_candidate_instruction_scope/);
  assert.match(statusPage, /complete_candidate_instruction_scope/);
  assert.doesNotMatch(statusPage, /pay_payment_cancelability_diagnostic\s*\(/);

  const classifiedStart = statusPage.indexOf('candidate_classified_index AS MATERIALIZED');
  const classifiedEnd = statusPage.indexOf('candidate_filtered_index AS MATERIALIZED', classifiedStart);
  const classified = statusPage.slice(classifiedStart, classifiedEnd);
  assert.match(classified, /WHEN release_failed_payment_eligible THEN 'NOT_PAID'/);
  assert.match(classified, /WHEN release_failed_payment_eligible THEN ARRAY\['RELEASE_FAILED_PAYMENT'\]/);
  assert.match(classified, /WHEN canonical_provider_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN ARRAY\[\]::text\[\]/);
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
  assert.match(selectionPrepare, /v_candidate_selection_json \|\| pg_catalog\.jsonb_build_object\(/);
  assert.match(selectionPrepare, /'pay_batch_item_ids', pg_catalog\.to_jsonb\(v_item_ids\)/);
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
    scopeIsFull: true,
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
    { scopeIsFull: false },
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
    scopeIsFull: true,
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

test('unsent provider outage is retry-later and cannot become a cancellation or release action', () => {
  const unsentOutage = {
    paidOrSettled: false,
    terminalNoMoney: false,
    providerPendingNonFinal: false,
    providerOutcomeUnknown: false,
    providerOutage: true,
    providerRequestSent: false,
    providerExternalIdPresent: false,
    providerSubmissionInProgress: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };

  assert.equal(canonicalProviderState(unsentOutage), 'PROVIDER_OUTAGE_RETRY_LATER');
  assert.equal(eligibleForOrdinaryCancellation(unsentOutage), false);
  assert.equal(diagnosticCanPreProviderCancel(unsentOutage), false);
  assert.equal(eligibleForFailedRelease({ ...unsentOutage, terminalNoMoney: true }), false);
  assert.equal(diagnosticCanNoMoneyUnwind({ ...unsentOutage, terminalNoMoney: true }), false);

  assert.equal(canonicalProviderState({
    ...unsentOutage,
    providerRequestSent: true,
  }), 'PENDING_NON_FINAL');
  assert.equal(canonicalProviderState({
    ...unsentOutage,
    terminalNoMoney: true,
    providerRequestSent: true,
  }), 'TERMINAL_NO_MONEY');
});

test('same-batch null-transfer events use the same provider precedence as linked events', () => {
  const common = {
    paidOrSettled: false,
    terminalNoMoney: false,
    providerPendingNonFinal: false,
    providerOutcomeUnknown: false,
    providerOutage: false,
    providerRequestSent: false,
    providerExternalIdPresent: false,
    providerSubmissionInProgress: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };

  assert.equal(canonicalProviderState({ ...common, unscopedFinalPaid: true }), 'FINAL_PAID');
  assert.equal(canonicalProviderState({ ...common, unscopedProviderOutcomeUnknown: true }), 'PROVIDER_OUTCOME_UNKNOWN');
  assert.equal(canonicalProviderState({ ...common, unscopedPendingNonFinal: true }), 'PENDING_NON_FINAL');
  assert.equal(canonicalProviderState({ ...common, unscopedProviderOutage: true }), 'PROVIDER_OUTAGE_RETRY_LATER');
  assert.equal(canonicalProviderState({ ...common, unscopedTerminalNoMoney: true }), 'TERMINAL_NO_MONEY');
  assert.equal(canonicalProviderState({
    ...common,
    unscopedTerminalNoMoney: true,
    unscopedPendingNonFinal: true,
  }), 'TERMINAL_NO_MONEY');
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
    { id: 'terminal-plus-unsent-outage', ...common, terminalNoMoney: true, providerOutage: true },
    { id: 'outage-only', ...common, providerOutage: true },
    { id: 'unscoped-terminal', ...common, unscopedTerminalNoMoney: true },
    { id: 'unscoped-terminal-plus-pending', ...common, unscopedTerminalNoMoney: true, unscopedPendingNonFinal: true },
    { id: 'unscoped-unknown', ...common, unscopedProviderOutcomeUnknown: true },
    { id: 'unscoped-pending', ...common, unscopedPendingNonFinal: true },
    { id: 'unscoped-outage', ...common, unscopedProviderOutage: true },
    { id: 'terminal-plus-paid', ...common, terminalNoMoney: true, paidOrSettled: true },
    { id: 'pending-only', ...common, providerPendingNonFinal: true },
    { id: 'unknown-only', ...common, providerOutcomeUnknown: true },
    { id: 'blocked-work', ...common, terminalNoMoney: true, latestWorkStatus: 'BLOCKED' },
    { id: 'failed-final-work', ...common, terminalNoMoney: true, latestWorkStatus: 'FAILED_FINAL' },
    { id: 'failed-retryable-work', ...common, terminalNoMoney: true, latestWorkStatus: 'FAILED_RETRYABLE' },
    { id: 'manual-blocker', ...common, terminalNoMoney: true, manualCarryForwardBlocked: true },
    { id: 'freshness-blocker', ...common, terminalNoMoney: true, carryForwardFreshnessBlocked: true },
    { id: 'partial-shared-scope', ...common, terminalNoMoney: true, scopeIsFull: false },
    { id: 'active', ...common },
  ];

  const reviewedIds = fixtures.filter(eligibleForFailedRelease).map(({ id }) => id).sort();
  const preparedIds = fixtures.filter(diagnosticCanNoMoneyUnwind).map(({ id }) => id).sort();
  assert.deepEqual(reviewedIds.filter((id) => !preparedIds.includes(id)), []);
  assert.deepEqual(preparedIds.filter((id) => !reviewedIds.includes(id)), []);
  assert.deepEqual(reviewedIds, [
    'plain-terminal',
    'terminal-plus-pending',
    'unscoped-terminal',
    'unscoped-terminal-plus-pending',
  ]);
});

test('ordinary cancellation has one fail-closed reviewed authority and preparation excludes prior failed work', () => {
  assert.match(statusPage, /AS pre_provider_cancel_eligible/);
  assert.match(statusPage, /pre_provider_cancel_eligible THEN ARRAY\['CANCEL_PAYMENT'\]/);
  assert.match(statusPage, /active_item_count > 0/);
  assert.match(statusPage, /manual_carry_forward_blocked IS NOT TRUE/);
  assert.match(statusPage, /carry_forward_freshness_blocked IS NOT TRUE/);
  assert.match(statusPage, /complete_candidate_instruction_scope/);

  const classifiedStart = statusPage.indexOf('candidate_classified_index AS MATERIALIZED');
  const classifiedEnd = statusPage.indexOf('candidate_filtered_index AS MATERIALIZED', classifiedStart);
  const classified = statusPage.slice(classifiedStart, classifiedEnd);
  assert.doesNotMatch(classified, /ELSE ARRAY\['CANCEL_PAYMENT'\]/);

  const actionStart = selectionPrepare.indexOf('v_action_allowed := CASE');
  const actionEnd = selectionPrepare.indexOf('v_effective_display_state := CASE', actionStart);
  const action = selectionPrepare.slice(actionStart, actionEnd);
  for (const branch of [
    "WHEN v_requested_action = 'DRAFT_CANCEL'",
    "WHEN v_requested_action IN ('PRE_BANK_CANCEL', 'CANCEL_PAYMENT')",
    "WHEN v_requested_action IN ('NO_MONEY_RELEASE', 'NO_MONEY_UNWIND')",
  ]) {
    const branchStart = action.indexOf(branch);
    assert.notEqual(branchStart, -1, branch);
    const nextBranch = action.indexOf('WHEN v_requested_action', branchStart + branch.length);
    const branchBody = action.slice(branchStart, nextBranch === -1 ? action.length : nextBranch);
    for (const status of ['BLOCKED', 'FAILED_FINAL', 'FAILED_RETRYABLE']) {
      assert.match(branchBody, new RegExp(`latest_work_status IS DISTINCT FROM '${status}'`));
    }
  }
});

test('locally prepared pending transfers remain cancellable until provider evidence exists', () => {
  const scheduledLocalPending = {
    activeItemCount: 1,
    scopeIsFull: true,
    providerPendingNonFinal: true,
    localPreparedOnly: true,
    providerRequestSent: false,
    providerEventPresent: false,
    providerResponsePresent: false,
    providerExternalIdPresent: false,
    providerSubmissionInProgress: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };

  assert.equal(canonicalProviderState(scheduledLocalPending), 'NO_TRANSFER_EVIDENCE');
  assert.equal(eligibleForOrdinaryCancellation(scheduledLocalPending), true);
  assert.equal(preparedOrdinaryCancellationEligible(scheduledLocalPending), true);

  const providerPending = { ...scheduledLocalPending, providerRequestSent: true };
  assert.equal(canonicalProviderState(providerPending), 'PENDING_NON_FINAL');
  assert.equal(eligibleForOrdinaryCancellation(providerPending), false);
  assert.equal(preparedOrdinaryCancellationEligible(providerPending), false);

  assert.match(statusPage, /provider_pending_non_final[\s\S]*provider_event_present/);
  assert.match(statusPage, /provider_external_id_present[\s\S]*provider_submission_in_progress/);
  const eligibilityStart = statusPage.indexOf('candidate_release_eligibility_index AS MATERIALIZED');
  const eligibilityEnd = statusPage.indexOf('candidate_classified_index AS MATERIALIZED', eligibilityStart);
  const eligibility = statusPage.slice(eligibilityStart, eligibilityEnd);
  assert.doesNotMatch(eligibility, /provider_pending_non_final IS NOT TRUE/);
  assert.match(
    catalogueFunctions,
    /CASE WHEN v_local_prepared_only THEN 0 ELSE v_transfer_pending_count END/
  );
});

test('operation and chunk provider payload aliases match provider-evidence empty and sentinel semantics', () => {
  for (const payload of [
    { provider_request_sent_confirmed: true },
    { request_dispatched: true },
    { provider_evidence: { provider_request_sent: true } },
    { diagnostic: { provider_request_sent: true } },
    { outcome: { provider_request_sent: true } },
    { provider_request_sent_count: 1 },
    { providerCallSentCount: 2 },
    { provider_submit_diagnostic: { provider_request_sent_count: 1 } },
    { provider_response: { accepted: true } },
    { provider_response: ['accepted'] },
    { provider_response: 200 },
  ]) {
    assert.equal(operationPayloadHasProviderRequest(payload), true, JSON.stringify(payload));
  }

  for (const payload of [
    {},
    { provider_response: {} },
    { provider_response: [] },
    { provider_response: null },
    { provider_response: '' },
    { provider_response: 'null' },
    { provider_response: 'false' },
    { provider_response: 'true' },
    { provider_response: 'none' },
    { provider_response: 'n/a' },
    { provider_request_sent_count: 0 },
  ]) {
    assert.equal(operationPayloadHasProviderRequest(payload), false, JSON.stringify(payload));
  }

  for (const marker of [
    'provider_request_sent_confirmed',
    'request_dispatched',
    "{provider_evidence,provider_request_sent}",
    "{diagnostic,provider_request_sent}",
    "{outcome,provider_request_sent}",
    'provider_call_sent_count',
    "provider_response') = 'object'",
    "provider_response') = 'array'",
    "NOT IN ('null','false','true','none','n/a','na')",
  ]) {
    assert.equal(statusPage.includes(marker), true, marker);
  }
});

test('reviewed and immutable ordinary-cancellation sets are equal in both directions', () => {
  const common = {
    activeItemCount: 1,
    scopeIsFull: true,
    paidOrSettled: false,
    terminalNoMoney: false,
    providerPendingNonFinal: false,
    providerOutcomeUnknown: false,
    providerOutage: false,
    providerRequestSent: false,
    providerEventPresent: false,
    providerResponsePresent: false,
    providerExternalIdPresent: false,
    providerSubmissionInProgress: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };
  const fixtures = [
    { id: 'plain-local', ...common },
    { id: 'empty-scope', ...common, activeItemCount: 0, scopeIsFull: false },
    { id: 'partial-shared-scope', ...common, activeItemCount: 1, scopeIsFull: false },
    { id: 'manual-carry-blocker', ...common, manualCarryForwardBlocked: true },
    { id: 'freshness-blocker', ...common, carryForwardFreshnessBlocked: true },
    { id: 'blocked-work', ...common, latestWorkStatus: 'BLOCKED' },
    { id: 'failed-final-work', ...common, latestWorkStatus: 'FAILED_FINAL' },
    { id: 'failed-retryable-work', ...common, latestWorkStatus: 'FAILED_RETRYABLE' },
    { id: 'active-provider-submission', ...common, providerSubmissionInProgress: true },
    { id: 'provider-event', ...common, providerEventPresent: true },
    { id: 'provider-response', ...common, providerResponsePresent: true },
    { id: 'provider-external-id', ...common, providerExternalIdPresent: true },
    { id: 'request-confirmed-alias', ...common, operationPayload: { provider_request_sent_confirmed: true }, providerRequestSent: true },
    { id: 'request-dispatched-alias', ...common, operationPayload: { request_dispatched: true }, providerRequestSent: true },
    { id: 'positive-request-count', ...common, operationPayload: { provider_request_sent_count: 1 }, providerRequestSent: true },
    { id: 'empty-provider-response', ...common, operationPayload: { provider_response: {} } },
    { id: 'sentinel-provider-response', ...common, operationPayload: { provider_response: 'none' } },
    { id: 'outage-only', ...common, providerOutage: true },
    { id: 'terminal', ...common, terminalNoMoney: true },
    { id: 'pending', ...common, providerPendingNonFinal: true },
    { id: 'unknown', ...common, providerOutcomeUnknown: true },
    { id: 'paid', ...common, paidOrSettled: true },
    { id: 'unscoped-outage', ...common, unscopedProviderOutage: true },
    { id: 'unscoped-terminal', ...common, unscopedTerminalNoMoney: true },
    { id: 'unscoped-pending', ...common, unscopedPendingNonFinal: true },
    { id: 'unscoped-unknown', ...common, unscopedProviderOutcomeUnknown: true },
  ];

  const reviewedIds = fixtures.filter(eligibleForOrdinaryCancellation).map(({ id }) => id).sort();
  const preparedIds = fixtures.filter(preparedOrdinaryCancellationEligible).map(({ id }) => id).sort();
  assert.deepEqual(reviewedIds.filter((id) => !preparedIds.includes(id)), []);
  assert.deepEqual(preparedIds.filter((id) => !reviewedIds.includes(id)), []);
  assert.deepEqual(reviewedIds, [
    'empty-provider-response',
    'plain-local',
    'sentinel-provider-response',
  ]);
});

test('draft cancellation uses the same provider, full-scope and prior-work authority in both readers', () => {
  const common = {
    activeItemCount: 1,
    scopeIsFull: true,
    paidOrSettled: false,
    terminalNoMoney: false,
    providerPendingNonFinal: false,
    providerOutcomeUnknown: false,
    providerOutage: false,
    providerRequestSent: false,
    providerEventPresent: false,
    providerResponsePresent: false,
    providerExternalIdPresent: false,
    providerSubmissionInProgress: false,
    latestWorkStatus: null,
    manualCarryForwardBlocked: false,
    carryForwardFreshnessBlocked: false,
  };
  const fixtures = [
    { id: 'local-full', ...common },
    { id: 'partial-scope', ...common, scopeIsFull: false },
    { id: 'pending', ...common, providerPendingNonFinal: true },
    { id: 'unknown', ...common, providerOutcomeUnknown: true },
    { id: 'submission', ...common, providerSubmissionInProgress: true },
    { id: 'outage', ...common, providerOutage: true },
    { id: 'paid', ...common, paidOrSettled: true },
    { id: 'blocked-work', ...common, latestWorkStatus: 'BLOCKED' },
    { id: 'failed-final-work', ...common, latestWorkStatus: 'FAILED_FINAL' },
    { id: 'failed-retryable-work', ...common, latestWorkStatus: 'FAILED_RETRYABLE' },
  ];
  const reviewed = fixtures.filter(eligibleForOrdinaryCancellation).map(({ id }) => id).sort();
  const prepared = fixtures.filter(preparedOrdinaryCancellationEligible).map(({ id }) => id).sort();
  assert.deepEqual(reviewed, ['local-full']);
  assert.deepEqual(prepared, reviewed);
  assert.match(statusPage, /WHEN v_batch\.status = 'DRAFT' AND pre_provider_cancel_eligible/);
  assert.match(selectionPrepare, /WHEN v_requested_action = 'DRAFT_CANCEL'[\s\S]*can_pre_provider_cancel/);
});

test('status readers expose only safe selection and actor-tailored progress contracts', () => {
  assert.match(statusPage, /v_explicit_snapshot_token/);
  assert.match(statusPage, /'filter', '\{\}'::jsonb/);
  assert.match(statusPage, /'explicit_snapshot_token', v_explicit_snapshot_token/);
  assert.doesNotMatch(statusPage, /'operation_id', \(\s*SELECT operation_row\.id/);
  assert.match(correctionStatus, /pay_payment_correction_request_candidates AS membership/);
  assert.match(correctionStatus, /v_can_authorise/);
  assert.match(correctionStatus, /v_can_use_golden_key/);
  assert.match(correctionStatus, /'selected_amount_pence'/);
  assert.match(correctionStatus, /'remaining_amount_pence'/);
  assert.match(correctionStatus, /'percent'/);
  assert.match(correctionStatus, /'label', blocker_summary\.label/);
  assert.doesNotMatch(correctionStatus, /'operation_id', v_operation\.id/);
  assert.doesNotMatch(correctionStatus, /'root_operation_id'/);
});
