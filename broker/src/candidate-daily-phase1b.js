import {
  candidateCorrelationId,
  dailyErrorResponse,
  findCandidateDailyRoute,
  isCandidateDailyPath,
  readBoundedDailyJson,
  rebuildCandidateDailySuccessBody,
  validateDailyIdempotency
} from './candidate-daily-contract-v1.js';
import { verifyCandidateDailySystemRequest } from './candidate-daily-hmac-v1.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const SHA256_RE = /^[a-f0-9]{64}$/;
const ITEM_KEY_RE = /^[A-Za-z0-9._~-]{8,160}$/;
const IDEMPOTENCY_RE = /^[A-Za-z0-9._~:+\-/]{16,128}$/;
const CORRELATION_RE = /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/;
const SOURCE_HMAC_RE = /^[a-f0-9]{64}$/;
const GLOBAL_CANDIDATE_KEY_RE = /^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$/;
const OPAQUE_TOKEN_RE = /^[A-Za-z0-9._~-]{16,512}$/;
const LEGACY_VALUES = new Set(['', 'N/A', 'LD', 'N', 'LD/N']);
const PREFERENCES = new Set(['PENDING', 'NOT_AVAILABLE', 'LONG_DAY', 'NIGHT', 'LONG_DAY_OR_NIGHT']);
const EFFECT_OPERATIONS = new Set([
  'RUNNING_LATE_SEND', 'CANNOT_ATTEND', 'LEAVE_EARLY', 'DNA', 'MESSAGE_SEEN',
  'ESCALATION_STEP', 'ACKNOWLEDGEMENT'
]);

const RPC_BY_OPERATION = Object.freeze({
  getCandidateDailyTiles: 'candidate_daily_tiles_get_v1',
  applyCandidateDailyAvailability: 'candidate_daily_availability_apply_atomic_v1',
  googleAvailabilityLegacyApply: 'candidate_daily_legacy_availability_apply_atomic_v1',
  googleAvailabilityLegacyStatus: 'candidate_daily_legacy_availability_status_get_v1',
  googleAvailabilityPublishRotaGenerations: 'candidate_daily_rota_generation_publish_atomic_v1',
  googleAvailabilityClaimProjection: 'candidate_daily_projection_claim_v1',
  googleAvailabilityCompleteProjection: 'candidate_daily_projection_complete_atomic_v1',
  googleAvailabilityReadSyncStatus: 'candidate_daily_sync_status_get_v1',
  googleAvailabilityApplySheetEdits: 'candidate_daily_reconciliation_apply_atomic_v1',
  googleAvailabilityApplyReconciliation: 'candidate_daily_reconciliation_apply_atomic_v1',
  googleAvailabilityEffectClaim: 'candidate_daily_external_effect_claim_v1',
  googleAvailabilityEffectComplete: 'candidate_daily_external_effect_complete_v1',
  googleAvailabilityEffectStatus: 'candidate_daily_external_effect_status_get_v1'
});

const ERROR_MAP = Object.freeze({
  AVAILABILITY_DATE_NOT_EDITABLE: [422, 'DO_NOT_RETRY'],
  AVAILABILITY_VERSION_CONFLICT: [409, 'REFRESH'],
  BATCH_IN_PROGRESS: [409, 'STATUS_CHECK'],
  CANDIDATE_DAILY_DISABLED: [403, 'REFRESH'],
  CANDIDATE_DAILY_NOT_READY: [503, 'STATUS_CHECK'],
  COMMAND_IN_PROGRESS: [409, 'STATUS_CHECK'],
  DAILY_GENERATION_UNAVAILABLE: [409, 'STATUS_CHECK'],
  EFFECT_NOT_FOUND: [404, 'DO_NOT_RETRY'],
  EFFECT_STATUS_UNKNOWN: [409, 'STATUS_CHECK'],
  FORBIDDEN: [403, 'DO_NOT_RETRY'],
  GENERATION_INCOMPLETE: [422, 'DO_NOT_RETRY'],
  IDEMPOTENCY_KEY_REUSED: [409, 'DO_NOT_RETRY'],
  IDENTITY_LINK_AMBIGUOUS: [409, 'DO_NOT_RETRY'],
  IDENTITY_LINK_CONFLICT: [409, 'DO_NOT_RETRY'],
  IDENTITY_LINK_MISSING: [409, 'STATUS_CHECK'],
  LEASE_CONFLICT: [409, 'STATUS_CHECK'],
  LEASE_EXPIRED_STATUS_REQUIRED: [409, 'STATUS_CHECK'],
  NOT_FOUND: [404, 'DO_NOT_RETRY'],
  PROJECTION_STALE_COMPLETION: [409, 'STATUS_CHECK'],
  SEMANTIC_REJECTION: [422, 'DO_NOT_RETRY'],
  SOURCE_EVENT_CONFLICT: [409, 'DO_NOT_RETRY'],
  SOURCE_IDENTITY_NOT_READY: [409, 'STATUS_CHECK'],
  VALIDATION_FAILED: [400, 'DO_NOT_RETRY']
});

function isObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function exactKeys(value, allowed, required = allowed) {
  return isObject(value) && Object.keys(value).every((key) => allowed.includes(key))
    && required.every((key) => Object.hasOwn(value, key));
}

function boundedString(value, minimum, maximum, pattern = null) {
  return typeof value === 'string' && value.length >= minimum && value.length <= maximum
    && (!pattern || pattern.test(value));
}

function safeInteger(value, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  return Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function isoDateTime(value) {
  return boundedString(value, 1, 64) && Number.isFinite(Date.parse(value));
}

function unwrapRpc(value, name) {
  let output = value;
  if (Array.isArray(output) && output.length === 1) output = output[0];
  if (isObject(output) && Object.hasOwn(output, name)) output = output[name];
  if (!isObject(output)) throw new Error('DEPENDENCY_UNAVAILABLE');
  return output;
}

async function rpc(deps, name, args) {
  if (!deps || typeof deps.rpc !== 'function') throw new Error('DEPENDENCY_UNAVAILABLE');
  return unwrapRpc(await deps.rpc(name, args, { timeoutMs: 10_000 }), name);
}

function successResponse(route, correlationId, result, replay = false) {
  const body = rebuildCandidateDailySuccessBody(route, 200, {
    ok: true,
    correlation_id: correlationId,
    result
  }, correlationId);
  if (!body) throw new Error('DEPENDENCY_UNAVAILABLE');
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
      'x-correlation-id': correlationId,
      'x-idempotent-replay': replay ? 'true' : 'false'
    }
  });
}

function knownErrorCode(error) {
  const text = [error?.json?.message, error?.json?.details, error?.body, error?.message, error]
    .map((value) => String(value || '')).join(' ');
  return Object.keys(ERROR_MAP).find((code) => new RegExp(`(?:^|[^A-Z0-9_])${code}(?:$|[^A-Z0-9_])`).test(text))
    || (/DEPENDENCY_UNAVAILABLE|timed out|timeout|fetch failed/i.test(text) ? 'DEPENDENCY_UNAVAILABLE' : 'INTERNAL_ERROR');
}

function errorResponse(error, route, correlationId) {
  const code = knownErrorCode(error);
  if (code === 'DEPENDENCY_UNAVAILABLE') {
    const candidate = String(route?.routeClass || '').startsWith('CANDIDATE_DAILY_');
    return dailyErrorResponse(503, candidate ? 'CANDIDATE_DAILY_NOT_READY' : 'DEPENDENCY_UNAVAILABLE',
      candidate ? 'STATUS_CHECK' : 'RETRY_AFTER', correlationId);
  }
  if (code === 'INTERNAL_ERROR') {
    const command = String(route?.routeClass || '').endsWith('_COMMAND');
    return dailyErrorResponse(500, code, command ? 'STATUS_CHECK' : 'RETRY_AFTER', correlationId);
  }
  const [status, defaultRetryClass] = ERROR_MAP[code];
  const retryClass = code === 'GENERATION_INCOMPLETE'
    && route?.operationId === 'applyCandidateDailyAvailability' ? 'REFRESH' : defaultRetryClass;
  const details = code === 'AVAILABILITY_VERSION_CONFLICT' && safeInteger(error?.current_availability_version, 0)
    ? { kind: 'CONFLICT', current_availability_version: error.current_availability_version }
    : undefined;
  return dailyErrorResponse(status, code, retryClass, correlationId, details);
}

function candidateContext(access) {
  const environment = String(access?.environment || '').trim().toUpperCase();
  const candidateId = String(access?.selected_candidate_id || '').trim();
  if (!['TEST', 'LIVE'].includes(environment) || !UUID_RE.test(candidateId)) throw new Error('FORBIDDEN');
  return { policy: 'CANDIDATE_SURFACE', environment, candidate_id: candidateId };
}

function systemContext(env, routeOperation = null) {
  const environment = String(env?.CANDIDATE_APP_ENVIRONMENT || '').trim().toUpperCase();
  if (!['TEST', 'LIVE'].includes(environment)) throw new Error('DEPENDENCY_UNAVAILABLE');
  return {
    policy: 'SIGNED_SYSTEM_SYNC',
    environment,
    system_auth_verified: true,
    nonce_consumed: true,
    environment_trusted: true,
    stable_operation_identity: true,
    approved_source_mapping: true,
    source_scope_ready: true,
    authority_mode_compatible: true,
    transition_ready: true,
    ...(routeOperation ? { route_operation: routeOperation } : {})
  };
}

function legacyContext(env, sourceHmac) {
  return {
    ...systemContext(env),
    policy: 'LEGACY_COMPAT',
    candidate_source_hmac: sourceHmac
  };
}

function bodyIdempotency(route, request, body) {
  const result = validateDailyIdempotency(route, request, body);
  if (!result.ok) throw new Error('VALIDATION_FAILED');
  return result.idempotencyKey;
}

async function requestBody(request, route) {
  const parsed = await readBoundedDailyJson(request.clone(), route.maxBodyBytes);
  if (!parsed.ok) throw new Error('VALIDATION_FAILED');
  return parsed.body;
}

function validateAvailabilityBody(body) {
  if (!exactKeys(body, ['expected_availability_version', 'changes'])
      || !safeInteger(body.expected_availability_version, 0) || !Array.isArray(body.changes)
      || body.changes.length < 1 || body.changes.length > 14) return false;
  const seen = new Set();
  return body.changes.every((item) => exactKeys(item, ['date', 'availability'])
    && DATE_RE.test(item.date) && PREFERENCES.has(item.availability)
    && !seen.has(item.date) && Boolean(seen.add(item.date)));
}

function noQueryParameters(url) {
  return [...url.searchParams.keys()].length === 0;
}

function validateSpecialistRequest(request, route, url, body) {
  const operationId = route.operationId;
  if (operationId === 'getCandidateDailyPastShifts') {
    const allowed = new Set(['cursor', 'limit']);
    if ([...url.searchParams.keys()].some((key) => !allowed.has(key))
        || [...allowed].some((key) => url.searchParams.getAll(key).length > 1)) return null;
    const cursor = url.searchParams.get('cursor');
    const limitText = url.searchParams.get('limit');
    const limit = limitText == null ? 50 : Number(limitText);
    return (cursor == null || OPAQUE_TOKEN_RE.test(cursor)) && safeInteger(limit, 1, 100)
      ? { cursor, limit } : null;
  }
  if (operationId === 'getCandidateDailyContent') {
    const kind = decodeURIComponent(url.pathname.split('/').pop() || '');
    if (['hospital-addresses', 'accommodation-contacts'].includes(kind)) {
      return noQueryParameters(url) ? { kind } : null;
    }
    if (kind !== 'candidate-message') return null;
    if ([...url.searchParams.keys()].some((key) => key !== 'platform')
        || url.searchParams.getAll('platform').length !== 1) return null;
    const platform = url.searchParams.get('platform');
    return ['IOS', 'ANDROID', 'WEB'].includes(platform) ? { kind, platform } : null;
  }
  if (operationId === 'getCandidateDailyEmergencyWindow') {
    return noQueryParameters(url) ? {} : null;
  }
  if (operationId === 'getCandidateDailyEffectStatus') {
    if (!noQueryParameters(url)) return null;
    const effectKey = decodeURIComponent(url.pathname.split('/').pop() || '');
    return OPAQUE_TOKEN_RE.test(effectKey) ? { effect_key: effectKey } : null;
  }
  if (!noQueryParameters(url) || !isObject(body)) return null;
  if (operationId === 'getCandidateDailyRunningLateOptions') {
    return exactKeys(body, ['emergency_shift_token']) && OPAQUE_TOKEN_RE.test(body.emergency_shift_token)
      ? { ...body } : null;
  }
  if (['previewCandidateDailyRunningLate', 'sendCandidateDailyRunningLate'].includes(operationId)) {
    return exactKeys(body, ['emergency_shift_token', 'running_late_option_token'])
      && OPAQUE_TOKEN_RE.test(body.emergency_shift_token)
      && OPAQUE_TOKEN_RE.test(body.running_late_option_token) ? { ...body } : null;
  }
  if (operationId === 'raiseCandidateDailyEmergency') {
    if (!['CANNOT_ATTEND', 'LEAVE_EARLY', 'DNA'].includes(body.type)
        || !OPAQUE_TOKEN_RE.test(body.emergency_shift_token)) return null;
    if (body.type === 'CANNOT_ATTEND') {
      return exactKeys(body, ['type', 'emergency_shift_token', 'reason_text'])
        && boundedString(body.reason_text, 3, 1000) ? { ...body } : null;
    }
    if (body.type === 'LEAVE_EARLY') {
      return exactKeys(body, ['type', 'emergency_shift_token', 'reason_text', 'leave_time'])
        && boundedString(body.reason_text, 3, 1000)
        && /^(NOW|([01][0-9]|2[0-3]):[0-5][0-9])$/.test(body.leave_time) ? { ...body } : null;
    }
    return exactKeys(body, ['type', 'emergency_shift_token', 'subject_token', 'tried_calling', 'reason_text'],
      ['type', 'emergency_shift_token', 'subject_token', 'tried_calling'])
      && OPAQUE_TOKEN_RE.test(body.subject_token) && body.tried_calling === true
      && (body.reason_text === undefined || boundedString(body.reason_text, 0, 1000)) ? { ...body } : null;
  }
  if (operationId === 'markCandidateDailyMessageSeen') {
    return exactKeys(body, ['message_token', 'mode']) && OPAQUE_TOKEN_RE.test(body.message_token)
      && ['LINK', 'EMBED', 'COMMIT', 'ALL'].includes(body.mode) ? { ...body } : null;
  }
  return null;
}

function validateSourceHmac(value) {
  return typeof value === 'string' && SOURCE_HMAC_RE.test(value);
}

function validateLegacyAvailability(body) {
  if (!exactKeys(body, ['candidate_source_hmac', 'request_id', 'changes'])
      || !validateSourceHmac(body.candidate_source_hmac) || !UUID_RE.test(body.request_id)
      || !Array.isArray(body.changes) || body.changes.length < 1 || body.changes.length > 14) return false;
  return body.changes.every((item) => exactKeys(item, ['date', 'availability'])
    && DATE_RE.test(item.date) && LEGACY_VALUES.has(item.availability));
}

function validateRotaDay(item) {
  const allowed = ['date', 'booked', 'system_blocked', 'booking_id', 'shift_starts_at', 'shift_ends_at',
    'shift_info', 'hospital', 'ward', 'job_title', 'source_row_hash'];
  if (!exactKeys(item, allowed, ['date', 'booked', 'system_blocked', 'source_row_hash'])
      || !DATE_RE.test(item.date) || typeof item.booked !== 'boolean'
      || typeof item.system_blocked !== 'boolean' || !SHA256_RE.test(item.source_row_hash)) return false;
  if (item.booked) {
    if (!boundedString(item.booking_id, 1, 128) || !isoDateTime(item.shift_starts_at)
        || !isoDateTime(item.shift_ends_at) || Date.parse(item.shift_ends_at) <= Date.parse(item.shift_starts_at)) return false;
  } else if (item.booking_id != null || item.shift_starts_at != null || item.shift_ends_at != null) return false;
  for (const key of ['shift_info', 'hospital', 'ward', 'job_title']) {
    if (item[key] !== undefined && item[key] !== null && !boundedString(item[key], 0, key === 'shift_info' ? 256 : 160)) return false;
  }
  return true;
}

function validateRotaGenerations(body) {
  if (!exactKeys(body, ['batch_request_id', 'items']) || !UUID_RE.test(body.batch_request_id)
      || !Array.isArray(body.items) || body.items.length < 1 || body.items.length > 50) return false;
  const keys = new Set();
  return body.items.every((item) => exactKeys(item, ['candidate_global_key', 'candidate_source_hmac',
    'source_hmac_key_version', 'source_event_id', 'source_revision', 'source_hash', 'window_start', 'days',
    'source_event_time', 'item_key'])
    && GLOBAL_CANDIDATE_KEY_RE.test(item.candidate_global_key)
    && validateSourceHmac(item.candidate_source_hmac) && item.source_hmac_key_version === 1
    && boundedString(item.source_event_id, 8, 160)
    && boundedString(item.source_revision, 1, 160) && SHA256_RE.test(item.source_hash)
    && DATE_RE.test(item.window_start) && isoDateTime(item.source_event_time) && ITEM_KEY_RE.test(item.item_key)
    && !keys.has(item.item_key) && Boolean(keys.add(item.item_key))
    && Array.isArray(item.days) && item.days.length === 14 && item.days.every(validateRotaDay));
}

function validateSheetEdits(body) {
  if (!exactKeys(body, ['batch_request_id', 'edits']) || !UUID_RE.test(body.batch_request_id)
      || !Array.isArray(body.edits) || body.edits.length < 1 || body.edits.length > 100) return false;
  const keys = new Set();
  return body.edits.every((item) => exactKeys(item, ['candidate_source_hmac', 'source_event_id', 'source_revision',
    'editor_hmac', 'sheet', 'cell', 'date', 'availability', 'source_event_time', 'source_hash', 'item_key'])
    && validateSourceHmac(item.candidate_source_hmac) && validateSourceHmac(item.editor_hmac)
    && boundedString(item.source_event_id, 8, 160) && boundedString(item.source_revision, 1, 160)
    && item.sheet === 'Availability' && /^[A-Z]{1,3}[1-9][0-9]{0,5}$/.test(item.cell)
    && DATE_RE.test(item.date) && LEGACY_VALUES.has(item.availability) && isoDateTime(item.source_event_time)
    && SHA256_RE.test(item.source_hash) && ITEM_KEY_RE.test(item.item_key)
    && !keys.has(item.item_key) && Boolean(keys.add(item.item_key)));
}

function validateProjectionClaim(body) {
  return exactKeys(body, ['claim_request_id', 'target', 'claimant', 'max_items'])
    && UUID_RE.test(body.claim_request_id) && body.target === 'MASTER_AVAILABILITY_SHEET'
    && boundedString(body.claimant, 8, 128) && safeInteger(body.max_items, 1, 100);
}

function validateProjectionComplete(body) {
  if (!exactKeys(body, ['batch_request_id', 'items']) || !UUID_RE.test(body.batch_request_id)
      || !Array.isArray(body.items) || body.items.length < 1 || body.items.length > 100) return false;
  const ids = new Set();
  return body.items.every((item) => exactKeys(item,
    ['outbox_id', 'lease_token', 'outcome', 'observed_sheet_revision', 'error_code'],
    ['outbox_id', 'lease_token', 'outcome']) && UUID_RE.test(item.outbox_id)
    && boundedString(item.lease_token, 16, 256)
    && ['DELIVERED', 'RETRY', 'DEFERRED_OVERLAY', 'TERMINAL'].includes(item.outcome)
    && !ids.has(item.outbox_id) && Boolean(ids.add(item.outbox_id))
    && (item.observed_sheet_revision === undefined || boundedString(item.observed_sheet_revision, 1, 160))
    && (item.error_code === undefined || boundedString(item.error_code, 1, 80))
    && (!['DELIVERED', 'DEFERRED_OVERLAY'].includes(item.outcome)
      || boundedString(item.observed_sheet_revision, 1, 160)));
}

function validateSyncStatus(body) {
  return exactKeys(body, ['candidate_source_hmacs']) && Array.isArray(body.candidate_source_hmacs)
    && body.candidate_source_hmacs.length >= 1 && body.candidate_source_hmacs.length <= 100
    && new Set(body.candidate_source_hmacs).size === body.candidate_source_hmacs.length
    && body.candidate_source_hmacs.every(validateSourceHmac);
}

function validateReconciliation(body) {
  if (!exactKeys(body, ['batch_request_id', 'observations']) || !UUID_RE.test(body.batch_request_id)
      || !Array.isArray(body.observations) || body.observations.length < 1 || body.observations.length > 100) return false;
  const keys = new Set();
  return body.observations.every((item) => exactKeys(item, ['candidate_source_hmac', 'date', 'observed_value',
    'observed_sheet_revision', 'source_event_id', 'source_revision', 'source_event_time', 'source_hash', 'item_key'])
    && validateSourceHmac(item.candidate_source_hmac) && DATE_RE.test(item.date)
    && LEGACY_VALUES.has(item.observed_value) && boundedString(item.observed_sheet_revision, 1, 160)
    && boundedString(item.source_event_id, 8, 160) && boundedString(item.source_revision, 1, 160)
    && isoDateTime(item.source_event_time) && SHA256_RE.test(item.source_hash) && ITEM_KEY_RE.test(item.item_key)
    && !keys.has(item.item_key) && Boolean(keys.add(item.item_key)));
}

function validateEffectClaim(body) {
  return exactKeys(body, ['effect_key', 'operation', 'candidate_source_hmac', 'request_hash', 'executor_id'])
    && boundedString(body.effect_key, 16, 256) && EFFECT_OPERATIONS.has(body.operation)
    && validateSourceHmac(body.candidate_source_hmac) && SHA256_RE.test(body.request_hash)
    && boundedString(body.executor_id, 8, 128);
}

function validateEffectComplete(body) {
  return exactKeys(body, ['effect_receipt_id', 'lease_token', 'outcome', 'provider_reference_hash', 'safe_result'],
    ['effect_receipt_id', 'lease_token', 'outcome']) && UUID_RE.test(body.effect_receipt_id)
    && boundedString(body.lease_token, 16, 256) && ['COMPLETED', 'FAILED_FINAL', 'UNKNOWN'].includes(body.outcome)
    && (body.provider_reference_hash === undefined || body.provider_reference_hash === null
      || SHA256_RE.test(body.provider_reference_hash))
    && (body.safe_result === undefined || body.safe_result === null
      || (exactKeys(body.safe_result, [], []) && Object.keys(body.safe_result).length === 0));
}

function validateSystemBody(operationId, body) {
  if (operationId === 'googleAvailabilityLegacyTiles') {
    return exactKeys(body, ['candidate_source_hmac', 'from', 'days'])
      && validateSourceHmac(body.candidate_source_hmac) && DATE_RE.test(body.from) && body.days === 14;
  }
  if (operationId === 'googleAvailabilityLegacyApply') return validateLegacyAvailability(body);
  if (operationId === 'googleAvailabilityLegacyTimesheetAuthorisationStatus') {
    return exactKeys(body, ['candidate_source_hmac', 'booking_id'])
      && validateSourceHmac(body.candidate_source_hmac) && boundedString(body.booking_id, 1, 128);
  }
  if (operationId === 'googleAvailabilityPublishRotaGenerations') return validateRotaGenerations(body);
  if (operationId === 'googleAvailabilityApplySheetEdits') return validateSheetEdits(body);
  if (operationId === 'googleAvailabilityClaimProjection') return validateProjectionClaim(body);
  if (operationId === 'googleAvailabilityCompleteProjection') return validateProjectionComplete(body);
  if (operationId === 'googleAvailabilityReadSyncStatus') return validateSyncStatus(body);
  if (operationId === 'googleAvailabilityApplyReconciliation') return validateReconciliation(body);
  if (operationId === 'googleAvailabilityLegacyStatus') {
    return exactKeys(body, ['candidate_source_hmac', 'request_id'])
      && validateSourceHmac(body.candidate_source_hmac) && UUID_RE.test(body.request_id);
  }
  if (operationId === 'googleAvailabilityEffectClaim') return validateEffectClaim(body);
  if (operationId === 'googleAvailabilityEffectComplete') return validateEffectComplete(body);
  if (operationId === 'googleAvailabilityEffectStatus') {
    return exactKeys(body, ['effect_key']) && boundedString(body.effect_key, 16, 256);
  }
  return false;
}

function cleanReplay(result) {
  if (!isObject(result)) return { result, replay: false };
  const replay = result._idempotent_replay === true;
  const clean = { ...result };
  delete clean._idempotent_replay;
  return { result: clean, replay };
}

function committedGenerationSourceHmacs(items, outcomes) {
  if (!Array.isArray(items) || !Array.isArray(outcomes)) return [];
  return [...new Set(outcomes
    .filter((outcome) => isObject(outcome)
      && safeInteger(outcome.index, 0, items.length - 1)
      && ['COMMITTED', 'REPLAYED'].includes(outcome.status))
    .map((outcome) => items[outcome.index]?.candidate_source_hmac)
    .filter(validateSourceHmac))];
}

function reconciledCandidateSourceHmacs(observations, outcomes) {
  if (!Array.isArray(observations) || !Array.isArray(outcomes)) return [];
  const linkedClassifications = new Set([
    'AMBIGUOUS',
    'CANONICAL_COMMAND_REQUIRED',
    'MATCH',
    'REPAIR_PROJECTION'
  ]);
  return [...new Set(outcomes
    .filter((outcome) => isObject(outcome)
      && safeInteger(outcome.index, 0, observations.length - 1)
      && linkedClassifications.has(outcome.classification))
    .map((outcome) => observations[outcome.index]?.candidate_source_hmac)
    .filter(validateSourceHmac))];
}

async function invokeSystemRpc(verification, env, deps) {
  const { route, body, correlationId, idempotencyKey } = verification;
  if (!validateSystemBody(route.operationId, body)) throw new Error('VALIDATION_FAILED');
  const common = systemContext(env);
  let result;
  let firstReadyActivation = null;
  let generationItems = null;
  switch (route.operationId) {
    case 'googleAvailabilityLegacyTiles': {
      const source = await rpc(deps, 'candidate_daily_tiles_get_v1', {
        p_internal_context: legacyContext(env, body.candidate_source_hmac), p_from: body.from, p_days: 14
      });
      result = {
        tiles: source.tiles.map((item) => ({
          ymd: item.date, displayDay: item.display_day, displayDate: item.display_date,
          booked: item.booked, editable: item.editable, status: item.status,
          ...(item.shift_info != null ? { shiftInfo: item.shift_info } : {}),
          ...(item.hospital != null ? { hospital: item.hospital } : {}),
          ...(item.ward != null ? { ward: item.ward } : {}),
          ...(item.job_title != null ? { jobTitle: item.job_title } : {}),
          ...(item.booking_ref != null ? { bookingRef: item.booking_ref } : {}),
          ...(item.shift_type != null ? { shiftType: item.shift_type } : {}),
          ...(item.booking_id != null ? { booking_id: item.booking_id } : {}),
          ...(item.timesheet_authorised != null ? { timesheet_authorised: item.timesheet_authorised } : {}),
          ...(item.timesheet_eligible != null ? { timesheet_eligible: item.timesheet_eligible } : {})
        })),
        lastLoadedAt: source.freshness.generation_published_at
      };
      break;
    }
    case 'googleAvailabilityLegacyTimesheetAuthorisationStatus': {
      const source = await rpc(deps, 'candidate_daily_tiles_get_v1', {
        p_internal_context: legacyContext(env, body.candidate_source_hmac), p_from: null, p_days: 14
      });
      const tile = source.tiles.find((item) => item.booking_id === body.booking_id);
      if (!tile) throw new Error('NOT_FOUND');
      result = {
        booking_id: body.booking_id,
        authorised: tile.timesheet_authorised === true,
        status_version: source.generation_version
      };
      break;
    }
    case 'googleAvailabilityLegacyApply':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: legacyContext(env, body.candidate_source_hmac),
        p_candidate_source_hmac: body.candidate_source_hmac, p_request_id: body.request_id,
        p_idempotency_key: idempotencyKey, p_changes: body.changes, p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityLegacyStatus':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: legacyContext(env, body.candidate_source_hmac),
        p_candidate_source_hmac: body.candidate_source_hmac, p_request_id: body.request_id,
        p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityPublishRotaGenerations':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: common, p_batch_request_id: body.batch_request_id,
        p_idempotency_key: idempotencyKey, p_items: body.items, p_correlation_id: correlationId
      });
      generationItems = body.items;
      break;
    case 'googleAvailabilityApplySheetEdits':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: systemContext(env, 'SHEET_EDIT_INGEST'), p_batch_request_id: body.batch_request_id,
        p_idempotency_key: idempotencyKey, p_observations: body.edits, p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityClaimProjection':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: common, p_claim_request_id: body.claim_request_id,
        p_idempotency_key: idempotencyKey, p_target: body.target, p_claimant: body.claimant,
        p_max_items: body.max_items, p_lease_seconds: 600, p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityCompleteProjection':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: common, p_batch_request_id: body.batch_request_id,
        p_idempotency_key: idempotencyKey, p_items: body.items, p_correlation_id: correlationId
      });
      firstReadyActivation = {
        p_candidate_source_hmacs: [],
        p_projection_outbox_ids: [...new Set(body.items
          .filter((item) => ['DELIVERED', 'DEFERRED_OVERLAY'].includes(item.outcome))
          .map((item) => item.outbox_id))]
      };
      break;
    case 'googleAvailabilityReadSyncStatus':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: common, p_candidate_source_hmacs: body.candidate_source_hmacs,
        p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityApplyReconciliation':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: systemContext(env, 'RECONCILIATION'), p_batch_request_id: body.batch_request_id,
        p_idempotency_key: idempotencyKey, p_observations: body.observations, p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityEffectClaim':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: common, p_effect_key: body.effect_key, p_operation: body.operation,
        p_candidate_source_hmac: body.candidate_source_hmac, p_request_hash: body.request_hash,
        p_executor_id: body.executor_id, p_idempotency_key: idempotencyKey, p_lease_seconds: 120,
        p_now_utc: new Date().toISOString(), p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityEffectComplete':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: common, p_effect_receipt_id: body.effect_receipt_id,
        p_lease_token: body.lease_token, p_outcome: body.outcome,
        p_provider_reference_hash: body.provider_reference_hash ?? null,
        p_safe_result: body.safe_result ?? null, p_now_utc: new Date().toISOString(),
        p_correlation_id: correlationId
      });
      break;
    case 'googleAvailabilityEffectStatus':
      result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: common, p_effect_key: body.effect_key,
        p_now_utc: new Date().toISOString(), p_correlation_id: correlationId
      });
      break;
    default:
      throw new Error('VALIDATION_FAILED');
  }
  const cleaned = cleanReplay(result);
  const response = successResponse(route, correlationId, cleaned.result, cleaned.replay);
  if (generationItems) {
    firstReadyActivation = {
      p_candidate_source_hmacs: committedGenerationSourceHmacs(generationItems, cleaned.result.outcomes),
      p_projection_outbox_ids: []
    };
  }
  if (route.operationId === 'googleAvailabilityApplyReconciliation') {
    firstReadyActivation = {
      p_candidate_source_hmacs: reconciledCandidateSourceHmacs(
        body.observations,
        cleaned.result.outcomes
      ),
      p_projection_outbox_ids: []
    };
  }
  if (firstReadyActivation
      && (firstReadyActivation.p_candidate_source_hmacs.length > 0
        || firstReadyActivation.p_projection_outbox_ids.length > 0)) {
    await rpc(deps, 'candidate_daily_system_policy_activate_ready_v1', {
      p_internal_context: systemContext(env, 'FIRST_READY_ACTIVATION'),
      ...firstReadyActivation,
      p_correlation_id: correlationId
    });
  }
  return response;
}

export function composeCandidateBootstrapPhase1b(bootstrap) {
  const source = isObject(bootstrap) ? bootstrap : {};
  const capabilities = isObject(source.capabilities) ? source.capabilities : {};
  const daily = isObject(capabilities.daily_availability)
    && typeof capabilities.daily_availability.enabled === 'boolean'
    && Object.keys(capabilities.daily_availability).every((key) => ['enabled', 'unavailable_reason'].includes(key))
    ? capabilities.daily_availability
    : { enabled: false, unavailable_reason: 'AUTHORITY_UNREADABLE' };
  return { ...source, capabilities: { ...capabilities, daily_availability: { ...daily } } };
}

export function candidateBootstrapCorrelation(request) {
  return candidateCorrelationId(request);
}

export async function handleCandidateDailyPhase1bRequest(request, access, env, deps) {
  const url = new URL(request.url);
  if (!isCandidateDailyPath(url.pathname)) return null;
  const correlationId = candidateCorrelationId(request);
  const route = findCandidateDailyRoute(request.method, url.pathname);
  if (!route || route.signedSystem) return dailyErrorResponse(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY', correlationId);
  if (!access?.session_id) return dailyErrorResponse(401, 'UNAUTHENTICATED', 'REAUTHENTICATE', correlationId);
  try {
    if (route.operationId === 'getCandidateDailyTiles') {
      const allowed = new Set(['from', 'days']);
      if ([...url.searchParams.keys()].some((key) => !allowed.has(key))
          || [...allowed].some((key) => url.searchParams.getAll(key).length > 1)) throw new Error('VALIDATION_FAILED');
      const from = url.searchParams.get('from');
      const daysText = url.searchParams.get('days');
      if ((from != null && !DATE_RE.test(from)) || (daysText != null && daysText !== '14')) throw new Error('VALIDATION_FAILED');
      const result = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: candidateContext(access), p_from: from, p_days: 14
      });
      return successResponse(route, correlationId, cleanReplay(result).result, false);
    }
    if (route.operationId === 'applyCandidateDailyAvailability') {
      const body = await requestBody(request, route);
      if (!validateAvailabilityBody(body)) throw new Error('VALIDATION_FAILED');
      const key = bodyIdempotency(route, request, body);
      const raw = await rpc(deps, RPC_BY_OPERATION[route.operationId], {
        p_internal_context: candidateContext(access), p_idempotency_key: key,
        p_expected_availability_version: body.expected_availability_version,
        p_changes: body.changes, p_correlation_id: correlationId
      });
      if (raw.error_code === 'AVAILABILITY_VERSION_CONFLICT') {
        const error = new Error(raw.error_code);
        error.current_availability_version = Number(raw.current_availability_version);
        throw error;
      }
      if (typeof raw.error_code === 'string') throw new Error(raw.error_code);
      const cleaned = cleanReplay(raw);
      return successResponse(route, correlationId, cleaned.result, cleaned.replay);
    }

    // Phase 1B freezes typed specialist seams without creating a generic Google proxy.
    // The Google-dependent implementation is deliberately supplied only in Phase 3.
    const specialistBody = ['GET', 'HEAD'].includes(request.method) ? undefined : await requestBody(request, route);
    const input = validateSpecialistRequest(request, route, url, specialistBody);
    if (input === null) throw new Error('VALIDATION_FAILED');
    const idempotencyKey = route.idempotency === 'REQUIRED'
      ? bodyIdempotency(route, request, specialistBody) : null;
    if (typeof deps?.candidateDailySpecialist !== 'function') {
      await rpc(deps, 'candidate_daily_tiles_get_v1', {
        p_internal_context: candidateContext(access), p_from: null, p_days: 14
      });
      throw new Error('DEPENDENCY_UNAVAILABLE');
    }
    const specialist = await deps.candidateDailySpecialist({
      operation_id: route.operationId,
      input,
      idempotency_key: idempotencyKey,
      correlation_id: correlationId,
      candidate_context: candidateContext(access)
    });
    if (!exactKeys(specialist, ['result', 'idempotent_replay'], ['result'])
        || (specialist.idempotent_replay !== undefined && typeof specialist.idempotent_replay !== 'boolean')) {
      throw new Error('DEPENDENCY_UNAVAILABLE');
    }
    return successResponse(route, correlationId, specialist.result, specialist.idempotent_replay === true);
  } catch (error) {
    return errorResponse(error, route, correlationId);
  }
}

export async function handleCandidateDailySystemPhase1bRequest(request, env, deps, options = {}) {
  const verification = await verifyCandidateDailySystemRequest(request, env, options);
  if (!verification.ok) {
    const correlationId = verification.correlationId || String(request.headers.get('x-correlation-id') || '');
    return dailyErrorResponse(verification.status, verification.errorCode, verification.retryClass, correlationId);
  }
  try {
    return await invokeSystemRpc(verification, env, deps);
  } catch (error) {
    return errorResponse(error, verification.route, verification.correlationId);
  }
}

export const candidateDailyPhase1bInternals = Object.freeze({
  ERROR_MAP,
  RPC_BY_OPERATION,
  candidateContext,
  cleanReplay,
  exactKeys,
  invokeSystemRpc,
  knownErrorCode,
  legacyContext,
  systemContext,
  validateAvailabilityBody,
  validateSpecialistRequest,
  validateSystemBody
});
