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

export function decodeSourceNativeDeferredReleaseProof(value, expected = {}) {
 const keys=['version','application_id','member_key','local_publication_request_id','orchestration_run_id','generation_id','pending_bundle_id',
  'decision_bundle_id','bundle_revision','original_pending_step_id','released_receipt_id','released_receipt_digest','head_ids','current_local_receipt_sha256','current_native_result_sha256'];
 if(!sourceNativeClosed(value,keys)||value.version!=='SOURCE_NATIVE_DEFERRED_RELEASE_PROOF_V1'||!member(value.member_key)
  ||!identities(value,['application_id','local_publication_request_id','orchestration_run_id','generation_id','pending_bundle_id','decision_bundle_id','original_pending_step_id','released_receipt_id'])
  ||!sourceNativePositiveBigint(value.bundle_revision)||!['released_receipt_digest','current_local_receipt_sha256','current_native_result_sha256'].every(k=>isHash(value[k]))
  ||!Array.isArray(value.head_ids)||value.head_ids.length!==1||!value.head_ids.every(isUuid)
  ||!equalExpected(value,expected,['application_id','member_key','local_publication_request_id','orchestration_run_id','generation_id','pending_bundle_id','decision_bundle_id','bundle_revision']))invalid('actual deferred release proof');
 return value;
}
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

const SOURCE_CALCULATION_CONTEXT_KEYS = Object.freeze(['action', 'family_id', 'family_bound_version',
  'current_target_vector_sha256', 'source_cycle_id', 'source_group_id', 'agency_id', 'candidate_id', 'client_id',
  'contract_id', 'contract_week_id', 'root_timesheet_id', 'work_event_id', 'week_ending_date', 'source_mode', 'policy',
  'timesheet', 'contract_week', 'contract_record', 'current_financial', 'approved_component_identity_components',
  'requires_zero_financial', 'protected_schedule', 'source_segments', 'protected_decisions', 'source_proposal',
  'client_sources', 'source_expenses', 'candidate_submission', 'current_comparison_revision_id', 'current_final_revision_id',
  'root_financial', 'provider', 'expected_head_revision', 'request_sequence', 'zero_rate_source_refs', 'next_source_segments',
  'protected_current_placement', 'current_entitlement']);
function sourceCalculationContext(value, expected = {}) {
  if (!sourceNativeClosed(value, SOURCE_CALCULATION_CONTEXT_KEYS) || value.action !== 'WITHDRAW'
      || !identities(value, ['family_id', 'source_cycle_id', 'source_group_id', 'agency_id', 'candidate_id', 'client_id',
        'contract_id', 'contract_week_id', 'root_timesheet_id', 'work_event_id'])
      || !sourceNativePositiveBigint(value.family_bound_version)
      || !(value.next_source_segments === null || Array.isArray(value.next_source_segments))
      || !['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY'].includes(value.source_mode)
      || !equalExpected(value, expected, ['family_id', 'work_event_id', 'root_timesheet_id'])) invalid('calculation context');
  decodeSourceProtectedCurrentPlacement(value.protected_current_placement, { family_id: value.family_id,
    original_root_timesheet_id: value.root_timesheet_id });
  return value;
}
export function decodeSourceUnprotectReviewContext(value, expected = {}) {
  if (!sourceNativeClosed(value, ['version', 'assembly_id', 'decision_id', 'member_key', 'source_phase',
    'source_observation_sha256', 'context_sha256', 'context']) || value.version !== 'SOURCE_UNPROTECT_REVIEW_CONTEXT_RESULT_V1'
      || !identities(value, ['assembly_id', 'decision_id']) || !member(value.member_key)
      || !['NO_FINAL_REPORT_YET', 'FINAL_SOURCE_OBSERVED'].includes(value.source_phase)
      || !isHash(value.source_observation_sha256) || !isHash(value.context_sha256)
      || !equalExpected(value, expected, ['assembly_id', 'decision_id', 'member_key'])) invalid('review context');
  sourceCalculationContext(value.context, expected);
  const proposal = value.context.source_proposal;
  if (!proposal || typeof proposal !== 'object'
      || (value.source_phase === 'NO_FINAL_REPORT_YET' && (proposal.source_revision !== null || proposal.source_hash !== null))
      || (value.source_phase === 'FINAL_SOURCE_OBSERVED' && (!isUuid(proposal.source_revision) || !isHash(proposal.source_hash)))) {
    invalid('source observation');
  }
  return value;
}
export function normalizeSourceNativeCalculationContext(value, expected = {}) {
  if (!sourceNativeClosed(value, [...SOURCE_CALCULATION_CONTEXT_KEYS, 'ok', 'contract', 'orchestration_run_id'])
      || value.ok !== true || value.contract !== 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1'
      || !isUuid(value.orchestration_run_id)
      || !equalExpected(value, expected, ['orchestration_run_id'])) invalid('native run context');
  const { ok, contract, orchestration_run_id, ...facts } = value;
  void ok; void contract; void orchestration_run_id;
  return sourceCalculationContext(facts, expected);
}

export function decodeSourcePreviewQuoteContext(value, expected = {}) {
  if (!sourceNativeClosed(value, ['version', 'review_context', 'quantity_partition', 'prior_commitment_cut_sha256'])
      || value.version !== 'SOURCE_PAY_DECISION_PREVIEW_QUOTE_CONTEXT_RESULT_V1' || !isHash(value.prior_commitment_cut_sha256)
      || !Array.isArray(value.quantity_partition) || !value.quantity_partition.length || value.quantity_partition.length > 1000) invalid('quote operands');
  decodeSourceUnprotectReviewContext(value.review_context, expected);
  sourceCutQuantityPartition(value.quantity_partition, expected);
  if (expected.prior_commitment_cut_sha256 !== undefined && value.prior_commitment_cut_sha256 !== expected.prior_commitment_cut_sha256) {
    invalid('quote cut digest');
  }
  return value;
}
function sourceCutQuantityPartition(quantities, expected) {
  if (!Array.isArray(quantities) || !quantities.length || quantities.length > 1000) invalid('cut quantity partition');
  const seen = new Set();let cutId;
  for (const row of quantities) {
    if (!sourceNativeClosed(row, ['cut_id', 'cut_member_no', 'member_sha256', 'family_root_id', 'source_row_id', 'source_work_event_id',
      'canonical_work_id', 'canonical_component_key', 'scope_kind', 'scope_mode', 'work_dates', 'supported_quantity_json', 'excluded_quantity_json'])
        || !identities(row, ['cut_id', 'source_row_id']) || !sourceNativePositiveBigint(row.cut_member_no)
        || BigInt(row.cut_member_no) > 2147483647n || !isHash(row.member_sha256)
        || !['family_root_id', 'source_work_event_id', 'canonical_work_id'].every(key => row[key] === null || isUuid(row[key]))
        || !(row.canonical_component_key === null || (typeof row.canonical_component_key === 'string' && row.canonical_component_key.length > 0))
        || !['TIMESHEET_FAMILY', 'SOURCE_ROW'].includes(row.scope_kind) || !['WHOLE_SHIFT', 'COMPONENT_QUANTITIES'].includes(row.scope_mode)
        || (row.scope_kind === 'TIMESHEET_FAMILY' && row.family_root_id === null)
        || !Array.isArray(row.work_dates) || !row.work_dates.every(date => typeof date === 'string' && DATE.test(date))
        || !['supported_quantity_json', 'excluded_quantity_json'].every(key => row[key] === null || typeof row[key] === 'object')
        || seen.has(row.cut_member_no) || (cutId !== undefined && cutId !== row.cut_id)) invalid('cut quantity partition');
    if (expected.cut_id !== undefined && row.cut_id !== expected.cut_id) invalid('quote cut identity');
    cutId = row.cut_id;seen.add(row.cut_member_no);
  }
}
export function decodeSourceUnprotectQuoteContext(value, expected = {}) {
  const keys = ['version','preparation_id','decision_id','review_id','member_key','accept_or_resume_request_id','orchestration_run_id',
    'context_sha256','source_observation_sha256','economic_target_sha256','quantity_partition','prior_commitment_cut_sha256'];
  if (!sourceNativeClosed(value, keys) || value.version !== 'SOURCE_UNPROTECT_QUOTE_CONTEXT_RESULT_V1'
      || !identities(value, ['preparation_id','decision_id','review_id','accept_or_resume_request_id','orchestration_run_id'])
      || !member(value.member_key) || !keys.slice(7, 10).every(key => isHash(value[key])) || !isHash(value.prior_commitment_cut_sha256)
      || !equalExpected(value, expected, ['preparation_id','decision_id','review_id','member_key','accept_or_resume_request_id','orchestration_run_id',
        'prior_commitment_cut_sha256'])) invalid('confirmed quote context');
  sourceCutQuantityPartition(value.quantity_partition, expected);
  return value;
}
export function decodeSourceCurrentFinancialSource(value, expected = {}) {
  const keys = ['version','family_id','work_event_id','source_cycle_id','placement_certificate_sha256','current_timesheet_id','current_contract_id','root_financial'];
  const fact = value?.root_financial;
  if (!sourceNativeClosed(value, keys) || value.version !== 'SOURCE_CURRENT_FINANCIAL_SOURCE_RESULT_V1'
      || !identities(value, ['family_id','work_event_id','source_cycle_id','current_timesheet_id','current_contract_id'])
      || !isHash(value.placement_certificate_sha256) || !equalExpected(value, expected, keys.slice(1, 7))
      || !sourceNativeClosed(fact, ['source_system','external_identity','external_revision','document_sha256','financial_row_id',
        'root_version','financial_timesheet_version','financial_revision_digest']) || fact.source_system !== 'CLOUDTMS_TIMESHEET_FINANCIAL'
      || !identities(fact, ['external_identity','financial_row_id']) || fact.external_identity !== fact.financial_row_id
      || !isHash(fact.document_sha256) || fact.document_sha256 !== fact.financial_revision_digest
      || !Number.isSafeInteger(fact.root_version) || fact.root_version < 1 || fact.root_version > 2147483647
      || fact.financial_timesheet_version !== fact.root_version || fact.external_revision !== String(fact.financial_timesheet_version)
      || !equalExpected(fact, expected, ['financial_row_id','root_version'])) invalid('current financial source');
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

export function decodeSourceUnprotectPreparationResult(value, expected = {}) {
  const keys = ['version', 'preparation_id', 'decision_id', 'member_key', 'review_id', 'accept_or_resume_request_id',
    'orchestration_run_id', 'payload_sha256', 'expires_at_utc'];
  if (!sourceNativeClosed(value, keys) || value.version !== 'SOURCE_UNPROTECT_PREPARATION_RESULT_V1'
      || !identities(value, ['preparation_id', 'decision_id', 'review_id', 'accept_or_resume_request_id', 'orchestration_run_id'])
      || !member(value.member_key) || !isHash(value.payload_sha256)
      || typeof value.expires_at_utc !== 'string' || !Number.isFinite(Date.parse(value.expires_at_utc))
      || !equalExpected(value, expected, keys.filter(key => !['version', 'expires_at_utc'].includes(key)))) invalid('registered preparation');
  return value;
}

export function decodeSourcePayDecisionConfirmationPrepared(value, expected = {}) {
  const keys = ['version', 'decision_id', 'review_id', 'request_id', 'confirmation_reason', 'members'];
  if (!sourceNativeClosed(value, keys) || value.version !== 'SOURCE_PAY_DECISION_CONFIRMATION_PREPARED_V1'
      || !identities(value, ['decision_id', 'review_id', 'request_id']) || !Array.isArray(value.members)
      || typeof value.confirmation_reason !== 'string' || !value.confirmation_reason.trim()
      || [...value.confirmation_reason].length > 1000
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
