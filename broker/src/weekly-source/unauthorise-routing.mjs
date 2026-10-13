// SP-04: native Source withdrawal owns replay, admission and financial refusal.
import { weeklySourceUnwrapRpc } from './authorise-routing.mjs';
import { normalizeBpayNextOwnerStatus } from '../bpay-next-legacy-fence.js';
import { recordGuardRefusalAfterRollback } from './guard-refusal-record.mjs';

export const WEEKLY_SOURCE_UNAUTHORISE_PROBE_RPC = 'weekly_source_first_authorisation_withdraw_available_v1';
export const WEEKLY_SOURCE_UNAUTHORISE_REQUEST_RPC = 'weekly_source_first_authorisation_withdraw_request_v1';
const NOT_MANAGED = 'WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT';
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const staleReasons = new Set(['ROW_SIGNATURE_MISMATCH', 'EXPECTED_TIMESHEET_MISMATCH', 'EXPECTED_ROW_SIGNATURE_BLANK', 'REQUESTED_ROW_NOT_CURRENT']);
const contextReasons = new Set(['ROOT_ID_REQUIRED', 'BOOKING_IDENTITY_MISSING', 'BOOKING_REFERENCE_CANONICAL_COLLISION', 'CANONICAL_AMBIGUOUS', 'CURRENT_ROW_CARDINALITY', 'VERSION_NOT_UNIQUE', 'CANONICAL_ROW_MISSING', 'CANDIDATE_UNRESOLVED', 'CONTRACT_WEEK_CARDINALITY', 'LIVE_AUTHORISATION_NOT_ON_CANONICAL_ROW', 'LIVE_AUTHORISATION_CARDINALITY', 'NO_REQUESTED_ROOTS', 'REQUESTED_ROOT_NULL_ELEMENT', 'REQUESTED_ROOT_DUPLICATE', 'ROOT_CANDIDATE_MISMATCH', 'BOOKING_REPOINTED_DURING_LOCK', 'ROOT_NOT_RETURNED_BY_RESOLVER', 'FAMILY_UNRESOLVED', 'FAMILY_RESULT_INCOMPLETE']);
const checkCodes = new Set(['WEEKLY_SOURCE_UNAUTHORISE_LATER_DECISION_EXISTS', 'WEEKLY_SOURCE_UNAUTHORISE_PAID', 'WEEKLY_SOURCE_UNAUTHORISE_MONEY_UNRESOLVED', 'WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE', 'WEEKLY_SOURCE_UNAUTHORISE_WRITE_OFF_UNRESOLVED', 'WEEKLY_SOURCE_UNAUTHORISE_RECOVERY_EXISTS', 'WEEKLY_SOURCE_UNAUTHORISE_INVOICED', 'WEEKLY_SOURCE_UNAUTHORISE_INVOICE_OPERATION_ACTIVE', 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE', NOT_MANAGED]);
function nativeChecks(result) {
  if (result.code === NOT_MANAGED) return Array.isArray(result.checks) && result.checks.length === 0;
  const valid = check => record(check) && /^W(?:[1-9]|10|11)$/.test(check.check || '')
    && typeof check.passed === 'boolean' && checkCodes.has(check.code)
    && ['PERMANENT', 'TEMPORARY', 'INTEGRITY'].includes(check.nature)
    && Array.isArray(check.reasons) && check.reasons.every(reason => typeof reason === 'string');
  if (!Array.isArray(result.checks) || result.checks.length !== 11 || !result.checks.every(valid)
      || new Set(result.checks.map(check => check.check)).size !== 11 || !Array.isArray(result.failed_checks)) return false;
  const failed = result.checks.filter(check => !check.passed);
  return failed.length > 0 && result.failed_checks.length === failed.length
    && result.failed_checks.every((check, index) => valid(check) && check.passed === false
      && check.check === failed[index].check && check.code === failed[index].code && check.nature === failed[index].nature
      && JSON.stringify(check.reasons) === JSON.stringify(failed[index].reasons))
    && failed.some(check => check.code === result.code && check.nature === result.refusal_nature);
}
function nativeRefusal(result, timesheetId) {
  if (!record(result) || result.ok !== false || result.withdrawn !== false || result.replayed !== false || typeof result.retryable !== 'boolean') return false;
  if (checkCodes.has(result.code) && ['PERMANENT', 'TEMPORARY', 'INTEGRITY'].includes(result.refusal_nature)
      && typeof result.refusal_message === 'string' && result.refusal_message.length > 0 && result.refusal_message.length <= 4000
      && result.retryable === (result.refusal_nature === 'TEMPORARY') && result.timesheet_id === timesheetId
      && uuid.test(result.canonical_timesheet_id || '') && nativeChecks(result)) return true;
  if (result.code === 'WEEKLY_SOURCE_UNAUTHORISE_REQUEST_INVALID') return !result.retryable && result.reason === 'TIMESHEET_ACTOR_AND_EXPECTED_ROW_SIGNATURE_REQUIRED';
  if (result.code === 'WEEKLY_SOURCE_UNAUTHORISE_ACTOR_INVALID') return !result.retryable && result.reason === 'ACTOR_NOT_FOUND_OR_INACTIVE';
  if (result.code === 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE') return !result.retryable && contextReasons.has(result.reason);
  if (result.code === 'WEEKLY_SOURCE_CANDIDATE_BUSY') return result.retryable && result.gate === 'BLOCKED' && uuid.test(result.candidate_id || '') && result.job_type === 'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL';
  if (result.code === 'WEEKLY_SOURCE_SERIAL_GATE_BYPASSED') return !result.retryable && typeof result.gate === 'string' && ['CANDIDATE_REQUIRED', 'JOB_TYPE_NOT_PINNED', 'GATE_DECISION_NOT_GRANTED'].includes(result.reason);
  const receiptReasons = result.code === 'WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_TAMPERED'
    ? ['MORE_THAN_ONE_RECEIPT_FOR_THE_REPLAY_IDENTITY', 'RECEIPT_DOES_NOT_REBUILD_TO_ITS_OWN_DIGEST']
    : result.code === 'WEEKLY_SOURCE_WITHDRAWAL_REPLAY_CONFLICT' ? ['EXPECTED_TIMESHEET_ID_DIFFERS_FROM_THE_RECORDED_REQUEST'] : [];
  return receiptReasons.includes(result.reason) && !result.retryable && result.refusal_nature === 'INTEGRITY'
    && result.review_required === true && typeof result.refusal_message === 'string' && result.refusal_message.length > 0 && result.refusal_message.length <= 4000;
}
async function ordinaryContractlessDaily({ rpc, readPhysicalTimesheet, timesheetId, actorUserId }) {
  const physical = await readPhysicalTimesheet(timesheetId);
  const presentation = weeklySourceUnwrapRpc(await rpc('weekly_source_office_timesheet_presentation_v1',
    { p_request: { actor_user_id: actorUserId, timesheet_id: timesheetId } }, { timeoutMs: 12000 }), 'weekly_source_office_timesheet_presentation_v1');
  return physical?.timesheet_id === timesheetId && physical.sheet_scope === 'DAILY' && physical.contract_id === null
    && presentation?.applicable === false && presentation.scope === 'DAILY'
    && presentation.lifecycle?.contract === 'WEEKLY_SOURCE_OFFICE_LIFECYCLE_V1'
    && presentation.lifecycle.ok === true && presentation.lifecycle.server_phase === 'DAILY';
}
const rollbackCodes = new Map([
  ['BPAY_NEXT_SOURCE_WITHDRAW_FINANCIAL_DEPENDENCY', '55000'],
  ['WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY', '55P03'],
  ['BPAY_NEXT_SOURCE_WITHDRAW_WORK_MISSING', '23514'],
  ['BPAY_NEXT_SOURCE_WITHDRAW_WORK_MISMATCH', '23514'],
  ...['WEEKLY_SOURCE_UNAUTHORISE_HEAD_LOCK_DISAGREEMENT', 'WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING',
    'WEEKLY_SOURCE_WITHDRAWAL_SIGNATURE_HANDOFF_INVALID', 'WEEKLY_SOURCE_WITHDRAWAL_SIGNATURE_HANDOFF_UNSAFE',
    'WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_FAILED', 'WEEKLY_SOURCE_UNAUTHORISE_OWNER_REFUSED',
    'WEEKLY_SOURCE_UNAUTHORISE_INVALIDATION_FAILED', 'WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED',
    'WEEKLY_SOURCE_UNAUTHORISE_HEAD_SUPERSESSION_FAILED', 'WEEKLY_SOURCE_UNAUTHORISE_HEAD_STILL_CURRENT',
    'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'].map(code => [code, '55000'])
]);
function refused(code, result = {}, status = 409) {
  const checks = result.failed_checks || result.checks;
  const failures = Array.isArray(checks) ? checks.filter(check => check?.passed === false) : [];
  const stale = failures.length === 1 && failures[0].check === 'W9' && Array.isArray(failures[0].reasons)
    && failures[0].reasons.length > 0 && failures[0].reasons.every(reason => staleReasons.has(reason));
  return { ordinary: false, status, body: { ok: false, success: false,
    source_outcome: stale ? 'STALE' : 'REFUSED', withdrawn: false, stale,
    error_code: code, code, refusal_nature: result.refusal_nature || null,
    refusal_message: result.refusal_message || null,
    retryable: result.retryable === true, outcome_unknown: false,
    lifecycle_changed: false, affected_rows: [],
    message: stale ? 'This Timesheet has changed; refresh it before trying again.'
      : code === 'BPAY_NEXT_SOURCE_WITHDRAW_FINANCIAL_DEPENDENCY'
        ? 'A payment depends on this Timesheet, so its authorisation cannot be withdrawn.'
        : code === 'WEEKLY_SOURCE_UNAUTHORISE_PAID'
          ? 'This Timesheet has been paid, so its authorisation cannot be withdrawn.'
          : code === 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY' || code === 'WEEKLY_SOURCE_CANDIDATE_BUSY'
            ? 'Another update is in progress; try again in a moment.'
            : 'This Timesheet cannot be unauthorised; check its details.' } };
}
export function weeklySourceUnauthoriseResultFields(value) {
  if (!record(value) || !['WITHDRAWN', 'REFUSED', 'STALE', 'UNKNOWN'].includes(value.source_outcome)) return {};
  return Object.fromEntries(['source_outcome', 'withdrawn', 'replayed', 'withdrawal_receipt_id',
    'refusal_nature', 'refusal_message', 'retryable', 'outcome_unknown'].filter(key => Object.hasOwn(value, key))
    .map(key => [key, value[key]]));
}
// An ordinary linked-pair mutation must qualify its complete, bounded native
// membership without withdrawing any Source work during the preflight.
export async function weeklySourceUnauthorisePairPreflight({ rpc, ownerStatusRpc, readPhysicalTimesheet, actorUserId, timesheetIds }) {
  let owner;
  try { owner = normalizeBpayNextOwnerStatus(await ownerStatusRpc()); }
  catch { return refused('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE', {}, 503); }
  if (owner.active_owner === 'LEGACY') return { ordinary: true };
  if (owner.active_owner !== 'NEXT') return refused('BPAY_NEXT_MODULE_NOT_ACTIVE');
  if (!Array.isArray(timesheetIds) || !timesheetIds.length || new Set(timesheetIds).size > 100 || timesheetIds.some(id => !uuid.test(id || ''))) return refused('WEEKLY_SOURCE_UNAUTHORISE_REQUEST_INVALID');
  for (const id of new Set(timesheetIds)) {
    try {
      const result = weeklySourceUnwrapRpc(await rpc(WEEKLY_SOURCE_UNAUTHORISE_PROBE_RPC,
        { p_timesheet_id: id }, { timeoutMs: 12000 }), WEEKLY_SOURCE_UNAUTHORISE_PROBE_RPC);
      if (result?.ok === false && result.available === false && result.code === NOT_MANAGED
          && result.timesheet_id === id && result.refusal_nature === 'PERMANENT'
          && result.reason === 'NO_LIVE_ROOT_AUTHORISATION' && result.retryable === false && Array.isArray(result.checks) && result.checks.length === 0) continue;
      if (result?.ok === false && result.available === false && result.timesheet_id === id
          && result.code === 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' && result.reason === 'CANDIDATE_UNRESOLVED'
          && result.refusal_nature === 'INTEGRITY' && result.retryable === false
          && await ordinaryContractlessDaily({ rpc, readPhysicalTimesheet, timesheetId: id, actorUserId })) continue;
    } catch { /* An unknown membership fact cannot permit an ordinary mutation. */ }
    return { ordinary: false, status: 409, body: { ok: false, success: false,
      error_code: 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED',
      message: 'This selection cannot be unauthorised together; check the Timesheets individually.',
      lifecycle_changed: false, affected_rows: [] } };
  }
  return { ordinary: true };
}
export async function weeklySourceUnauthoriseRouting({ rpc, ownerStatusRpc, readPhysicalTimesheet, timesheetId,
  expectedTimesheetId, expectedRowSignature, actorUserId }) {
  const unknown = () => ({ ordinary: false, status: 503, body: { ok: false, success: false,
    source_outcome: 'UNKNOWN', withdrawn: false, outcome_unknown: true, retryable: true,
    error_code: 'UNKNOWN_LIFECYCLE_OUTCOME', lifecycle_changed: false, affected_rows: [],
    message: 'The outcome could not be confirmed; try again to check the same request.' } });
  let owner;
  try { owner = normalizeBpayNextOwnerStatus(await ownerStatusRpc()); }
  catch { return unknown(); }
  if (owner.active_owner === 'LEGACY') return { ordinary: true };
  if (owner.active_owner !== 'NEXT') return refused('BPAY_NEXT_MODULE_NOT_ACTIVE');
  if (typeof rpc !== 'function' || !uuid.test(timesheetId || '') || !uuid.test(expectedTimesheetId || '')
      || !uuid.test(actorUserId || '') || typeof expectedRowSignature !== 'string' || !expectedRowSignature.trim()) {
    return refused('WEEKLY_SOURCE_UNAUTHORISE_REQUEST_INVALID', {}, 400);
  }
  // Advisory only. A failed/new NOT_MANAGED probe must never hide an old receipt.
  try { await rpc(WEEKLY_SOURCE_UNAUTHORISE_PROBE_RPC, { p_timesheet_id: timesheetId }, { timeoutMs: 12000 }); }
  catch { /* The original request owner still checks its receipt before fresh reads. */ }
  const original = Object.freeze({ actor_user_id: actorUserId, timesheet_id: timesheetId,
    expected_timesheet_id: expectedTimesheetId, expected_row_signature: expectedRowSignature });
  let result;
  const correlationId = `ws62-office:${globalThis.crypto.randomUUID()}`;
  try {
    result = weeklySourceUnwrapRpc(await rpc(WEEKLY_SOURCE_UNAUTHORISE_REQUEST_RPC,
      { p_request: original }, { timeoutMs: 20000 }), WEEKLY_SOURCE_UNAUTHORISE_REQUEST_RPC);
  } catch (error) {
    await recordGuardRefusalAfterRollback({ rpc, error, correlationId,
      caller: 'broker:weekly-source:weekly_source_first_authorisation_withdraw_request_v1', actorUserId });
    const database = error?.json;
    if (record(database) && rollbackCodes.has(database.message) && rollbackCodes.get(database.message) === database.code) {
      return refused(database.message, { retryable: database.message === 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY' });
    }
    return unknown();
  }
  if (!record(result) || typeof result.ok !== 'boolean') return unknown();
  // The native owner has already attempted the original receipt. Its context
  // cannot resolve a contractless Daily Candidate. The existing Office reader
  // positively qualifies that one ordinary surface, never historical absence.
  if (nativeRefusal(result, timesheetId) && result.code === 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      && result.reason === 'CANDIDATE_UNRESOLVED' && result.canonical_timesheet_id === timesheetId) {
    try {
      if (await ordinaryContractlessDaily({ rpc, readPhysicalTimesheet, timesheetId, actorUserId })) return { ordinary: true };
    } catch { /* Keep the definite native refusal; no unknown ordinary route. */ }
  }
  // This exact native result is reached after successful receipt replay. Never
  // infer ordinary ownership from available=false or a current unauthorised flag.
  if (nativeRefusal(result, timesheetId) && result.code === NOT_MANAGED && result.timesheet_id === timesheetId
      && result.refusal_nature === 'PERMANENT' && uuid.test(result.canonical_timesheet_id || '')) return { ordinary: true };
  if (result.ok === true && result.withdrawn === true && typeof result.replayed === 'boolean'
      && result.timesheet_id === timesheetId && result.expected_row_signature === expectedRowSignature
      && uuid.test(result.withdrawal_receipt_id || '') && result.canonical_timesheet_id === expectedTimesheetId
      && record(result.unauthorise_result) && result.unauthorise_result.ok === true
      && result.unauthorise_result.timesheet_id === result.canonical_timesheet_id
      && (result.unauthorise_result.contract_week_id === null || uuid.test(result.unauthorise_result.contract_week_id || ''))) {
    const native = result.unauthorise_result;
    return { ordinary: false, status: 200, body: { ok: true, success: true, authorised: false,
      unauthorised: true, source_outcome: 'WITHDRAWN', withdrawn: true, replayed: result.replayed,
      withdrawal_receipt_id: result.withdrawal_receipt_id, outcome_unknown: false,
      timesheet_id: timesheetId, current_timesheet_id: result.canonical_timesheet_id || timesheetId,
      contract_week_id: native.contract_week_id || null, refresh_required: true,
      lifecycle_changed: result.replayed !== true, no_change: result.replayed === true,
      affected_rows: [{ timesheet_id: result.canonical_timesheet_id || timesheetId,
        contract_week_id: native.contract_week_id || null, row_key: `timesheet:${timesheetId}` }] } };
  }
  if (nativeRefusal(result, timesheetId)) {
    return refused(result.code, result);
  }
  return unknown();
}
