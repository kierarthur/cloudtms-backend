// Private Source/native transport. These decoders do not confer database authority.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const LOCAL = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$/;
const MONEY = /^-?(0|[1-9]\d*)\.\d{2}$/;
const MAX_I64 = 9223372036854775807n;
const own = (value, key) => Object.hasOwn(value, key);
export const sourceNativeClosed = (value, keys) => value !== null && typeof value === 'object'
  && !Array.isArray(value) && Object.keys(value).length === keys.length && keys.every((key) => own(value, key));
const isUuid = (value) => typeof value === 'string' && UUID.test(value);
const isHash = (value) => typeof value === 'string' && HASH.test(value);
export const sourceNativePositiveBigint = (value) => typeof value === 'string'
  && /^[1-9]\d{0,18}$/.test(value) && BigInt(value) <= MAX_I64;
const member = (value) => typeof value === 'string' && value.length > 0 && value.length <= 512;
function invalid(label) {
  const error = new Error(`The saved Source ${label} is invalid.`);
  error.code = 'SOURCE_NATIVE_RESULT_INVALID';
  error.status = 502;
  throw error;
}
function identities(value, keys) { return keys.every((key) => isUuid(value[key])); }
function equalExpected(value, expected, keys) {
  return keys.every((key) => expected[key] === undefined || value[key] === expected[key]);
}

export function decodeSourceProtectedCurrentPlacement(value, expected = {}) {
  const keys = ['version', 'moved', 'family_id', 'family_root_id', 'member_key', 'original_root_timesheet_id',
    'original_contract_id', 'current_timesheet_id', 'current_contract_id', 'current_financial_id',
    'current_timesheet_version', 'current_approval_work_id', 'original_canonical_work_id', 'move_operation_id',
    'placement_revision', 'immutable_original_evidence_sha256', 'source_invoice_certificate_sha256', 'placement_certificate_sha256'];
  if (!sourceNativeClosed(value, keys) || value.version !== 'PROTECTED_CURRENT_PLACEMENT_V1'
      || typeof value.moved !== 'boolean' || !member(value.member_key)
      || !identities(value, ['family_id', 'family_root_id', 'original_root_timesheet_id', 'original_contract_id',
        'current_timesheet_id', 'current_contract_id'])
      || !['current_financial_id', 'current_approval_work_id', 'original_canonical_work_id', 'move_operation_id']
        .every(key => value[key] === null || isUuid(value[key]))
      || !Number.isSafeInteger(value.current_timesheet_version) || value.current_timesheet_version < 1
      || value.current_timesheet_version > 2147483647
      || !['immutable_original_evidence_sha256', 'source_invoice_certificate_sha256', 'placement_certificate_sha256']
        .every(key => isHash(value[key]))
      || !equalExpected(value, expected, ['family_id', 'family_root_id', 'member_key', 'original_root_timesheet_id',
        'current_timesheet_id', 'current_contract_id', 'placement_revision', 'placement_certificate_sha256'])) {
    invalid('protected current placement');
  }
  if (value.moved) {
    if (!isUuid(value.move_operation_id) || !sourceNativePositiveBigint(value.placement_revision)
        || value.original_root_timesheet_id === value.current_timesheet_id) invalid('accepted moved placement');
  } else if (value.move_operation_id !== null || value.placement_revision !== '0'
      || value.original_root_timesheet_id !== value.current_timesheet_id || value.original_contract_id !== value.current_contract_id
      || (value.current_approval_work_id !== null && value.current_approval_work_id !== value.original_canonical_work_id)) {
    invalid('original protected placement');
  }
  return value;
}

export function decodeSourceNativePrepared(value, expected = {}) {
  const keys = ['ok', 'outcome', 'family_id', 'orchestration_run_id', 'source_cycle_id', 'source_group_id',
    'agency_id', 'candidate_id', 'client_id', 'contract_id', 'root_timesheet_id', 'work_event_id',
    'week_start_date', 'week_ending_date', 'source_mode', 'family_bound_version', 'request_kind',
    'protected_schedule', 'run_state', 'idempotent_replay'];
  const schedule = value?.protected_schedule;
  if (!sourceNativeClosed(value, keys) || value.ok !== true || value.outcome !== 'PREPARED'
      || !identities(value, keys.slice(2, 12)) || value.request_kind !== 'WITHDRAW'
      || !sourceNativePositiveBigint(value.family_bound_version)
      || !['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY'].includes(value.source_mode)
      || !['RUNNING', 'COMPLETE', 'PENDING', 'FAILED', 'REFUSED'].includes(value.run_state)
      || typeof value.idempotent_replay !== 'boolean'
      || !DATE.test(value.week_start_date) || !DATE.test(value.week_ending_date)
      || !sourceNativeClosed(schedule, ['work_date', 'start_at_local', 'end_at_local', 'break_minutes'])
      || !DATE.test(schedule.work_date) || !LOCAL.test(schedule.start_at_local) || !LOCAL.test(schedule.end_at_local)
      || !Number.isSafeInteger(schedule.break_minutes) || schedule.break_minutes < 0
      || !equalExpected(value, expected, ['family_id', 'orchestration_run_id'])) invalid('native preparation');
  return value;
}

export function decodeSourceUnprotectRunPrepared(value, expected = {}) {
  const keys = ['version', 'preparation_id', 'decision_id', 'member_key', 'review_id',
    'accept_or_resume_request_id', 'route', 'preparation_state', 'expires_at_utc', 'prepared'];
  if (!sourceNativeClosed(value, keys) || value.version !== 'SOURCE_UNPROTECT_RUN_PREPARED_V1'
      || !identities(value, ['preparation_id', 'decision_id', 'review_id', 'accept_or_resume_request_id'])
      || !member(value.member_key) || !['NEXT', 'SOURCE_LOCAL'].includes(value.route)
      || !['RUN_PREPARED', 'PREPARED', 'CONSUMED', 'STALE'].includes(value.preparation_state)
      || typeof value.expires_at_utc !== 'string' || !Number.isFinite(Date.parse(value.expires_at_utc))
      || !equalExpected(value, expected, ['decision_id', 'member_key', 'review_id', 'accept_or_resume_request_id'])) {
    invalid('run preparation');
  }
  decodeSourceNativePrepared(value.prepared, expected);
  return value;
}

export function decodeSourcePayDecisionConfirmationPrepared(value, expected = {}) {
  const keys = ['version', 'decision_id', 'review_id', 'request_id', 'members'];
  if (!sourceNativeClosed(value, keys) || value.version !== 'SOURCE_PAY_DECISION_CONFIRMATION_PREPARED_V1'
      || !identities(value, ['decision_id', 'review_id', 'request_id']) || !Array.isArray(value.members)
      || !equalExpected(value, expected, ['decision_id', 'review_id', 'request_id'])) invalid('confirmation preparation');
  const seenMembers = new Set(); const seenRuns = new Set(); const seenPreparations = new Set();
  for (const result of value.members) {
    decodeSourceUnprotectRunPrepared(result, { decision_id: value.decision_id, review_id: value.review_id,
      accept_or_resume_request_id: value.request_id });
    if (seenMembers.has(result.member_key) || seenRuns.has(result.prepared.orchestration_run_id)
        || seenPreparations.has(result.preparation_id)) invalid('duplicate confirmation member');
    seenMembers.add(result.member_key); seenRuns.add(result.prepared.orchestration_run_id); seenPreparations.add(result.preparation_id);
  }
  return value;
}

function decodeNextControl(value) {
  const keys = ['family_id', 'orchestration_run_id', 'approval_id', 'generation_id', 'work_id', 'revision_id',
    'new_family_bound_version', 'approved_pay_ex_vat', 'source_manifest_sha256'];
  if (!sourceNativeClosed(value, keys) || !identities(value, keys.slice(0, 6))
      || !sourceNativePositiveBigint(value.new_family_bound_version)
      || typeof value.approved_pay_ex_vat !== 'string' || !MONEY.test(value.approved_pay_ex_vat)
      || !isHash(value.source_manifest_sha256)) invalid('NEXT control receipt');
}

function decodeLocalControl(value) {
  const keys = ['family_id', 'orchestration_run_id', 'approval_id', 'generation_id', 'publication_request_id',
    'new_family_bound_version', 'local_state', 'authority', 'requires_first_authorisation',
    'timesheet_financials_id', 'entitlement', 'local_receipt_sha256', 'native_result_sha256'];
  if (!sourceNativeClosed(value, keys) || !identities(value, keys.slice(0, 5))
      || !sourceNativePositiveBigint(value.new_family_bound_version)
      || !isHash(value.local_receipt_sha256) || !isHash(value.native_result_sha256)
      || !['COMPLETE', 'PENDING_FREEZE'].includes(value.local_state)
      || !['UNAUTHORISED_TSFIN', 'COMMON_ENTITLEMENT_HEAD'].includes(value.authority)
      || typeof value.requires_first_authorisation !== 'boolean') invalid('Local control receipt');
  if (value.authority === 'UNAUTHORISED_TSFIN') {
    if (value.local_state !== 'COMPLETE' || value.requires_first_authorisation !== true
        || !isUuid(value.timesheet_financials_id) || value.entitlement !== null) invalid('Local TSFIN receipt');
    return;
  }
  if (value.requires_first_authorisation !== false || value.timesheet_financials_id !== null) invalid('Local head receipt');
  const entitlement = value.entitlement;
  if (value.local_state === 'COMPLETE') {
    if (!sourceNativeClosed(entitlement, ['kind', 'decision_bundle_id', 'bundle_revision', 'source_head_id', 'publication_receipt_id'])
        || entitlement.kind !== 'COMMITTED_HEAD'
        || !identities(entitlement, ['decision_bundle_id', 'source_head_id', 'publication_receipt_id'])
        || !sourceNativePositiveBigint(entitlement.bundle_revision)) invalid('committed head reference');
  } else if (!sourceNativeClosed(entitlement, ['kind', 'pending_bundle_id', 'decision_bundle_id', 'bundle_revision'])
      || entitlement.kind !== 'PENDING_BUNDLE' || !identities(entitlement, ['pending_bundle_id', 'decision_bundle_id'])
      || !sourceNativePositiveBigint(entitlement.bundle_revision)) invalid('pending bundle reference');
}

export function decodeSourceNativeComposedResult(value, expected = {}) {
  const keys = ['version', 'application_id', 'decision_id', 'request_id', 'review_id', 'member_key',
    'preparation_id', 'route', 'origin', 'control_status', 'native_receipt', 'failure_code'];
  if (!sourceNativeClosed(value, keys) || value.version !== 'SOURCE_NATIVE_COMPOSED_RESULT_V1'
      || !identities(value, ['application_id', 'decision_id', 'request_id', 'review_id', 'preparation_id'])
      || !member(value.member_key) || !['NEXT', 'SOURCE_LOCAL'].includes(value.route)
      || !['APPLIED_IN_THIS_TRANSACTION', 'ADOPTED_EXISTING', 'NOT_APPLIED_DEPENDENCY'].includes(value.origin)
      || !equalExpected(value, expected, ['application_id', 'decision_id', 'request_id', 'review_id', 'member_key', 'preparation_id', 'route'])) {
    invalid('composed result');
  }
  if (value.origin === 'NOT_APPLIED_DEPENDENCY') {
    if (value.control_status !== 'NOT_APPLIED' || value.native_receipt !== null
        || !['DEPENDENCY_UNAVAILABLE', 'RESULT_UNKNOWN'].includes(value.failure_code)) invalid('dependency result');
  } else {
    if (value.control_status !== 'WITHDRAWN' || value.failure_code !== null) invalid('withdrawal result');
    if (value.route === 'NEXT') decodeNextControl(value.native_receipt);
    else decodeLocalControl(value.native_receipt);
    if (!equalExpected(value.native_receipt, expected, ['family_id', 'orchestration_run_id'])) invalid('native identity');
  }
  return value;
}

// sbRpc preserves json/body/status/fn. A code on the outer Error is not SQL proof.
// Callers must pass the actual invoked function, never a function read from the error.
const NATIVE_FUNCTIONS = new Set(['weekly_exceptional_pay_next_publication_status_v1',
  'weekly_exceptional_pay_publish_next_v1', 'weekly_exceptional_pay_stage_c1_request_v1',
  'weekly_exceptional_pay_complete_local_v1']);
export function sourceNativeContinuationFromRpcError(error, expectedFunction) {
  return continuationFromRpcError(error, expectedFunction, NATIVE_FUNCTIONS);
}
export function sourcePayDecisionContinuationFromRpcError(error, expectedFunction) {
  return continuationFromRpcError(error, expectedFunction,
    new Set(['weekly_source_pay_decision_confirmation_prepare_v1']));
}
function continuationFromRpcError(error, expectedFunction, allowedFunctions) {
  if (!allowedFunctions.has(expectedFunction) || error?.fn !== expectedFunction
      || error?.json?.code !== 'P0001' || error.json.message !== 'SOURCE_PAY_DECISION_RESULT_REQUIRED'
      || typeof error.json.details !== 'string') return null;
  let detail;
  try { detail = JSON.parse(error.json.details); } catch { return null; }
  if (!sourceNativeClosed(detail, ['version', 'decision_id', 'request_id', 'reason'])
      || detail.version !== 'SOURCE_NATIVE_CONTINUATION_V1' || !identities(detail, ['decision_id', 'request_id'])
      || !['NOT_APPLIED_DEPENDENCY', 'APPLICATION_INCOMPLETE', 'NATIVE_RESULT_UNKNOWN'].includes(detail.reason)) return null;
  return Object.freeze({ ...detail });
}

export function sourcePayDecisionReadAction(decisionId, requestId) {
  if (!isUuid(decisionId) || !isUuid(requestId)) invalid('saved result identity');
  return Object.freeze({ kind: 'READ_RESULT', target_type: 'SOURCE_PAY_DECISION',
    target_id: decisionId, caption: 'Check saved result', enabled: true, disabled_reason: null,
    url: `/api/weekly-source/pay-decisions/${decisionId}?request_id=${requestId}` });
}
