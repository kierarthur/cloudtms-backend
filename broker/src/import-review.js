const IMPORT_REVIEW_DB_CONTRACT = 'IMPORT_REVIEW_DB_V1';
const IMPORT_REVIEW_APPLY_CONTRACT = 'IMPORT_REVIEW_APPLY_V1';
const IMPORT_REVIEW_OPERATION_CONTRACT = 'IMPORT_APPLY_OPERATION_V2';
const IMPORT_REVIEW_CORRECTION_CONTRACT = 'IMPORT_CORRECTION_OPERATION_V2';
const IMPORT_REVIEW_FOLLOW_UP_COMPONENT_CONTRACT = 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_V1';
const IMPORT_REVIEW_TSFIN_SETTLEMENT_CONTRACT = 'IMPORT_REVIEW_TSFIN_SETTLEMENT_V1';
const IMPORT_REVIEW_INCREMENTAL_APPLY_CONTRACT = 'IMPORT_REVIEW_INCREMENTAL_APPLY_V1';
const IMPORT_REVIEW_UI_CONTRACT = 'IMPORT_REVIEW_UI_V6';
const IMPORT_REVIEW_EMAIL_GROUPING_CONTRACT = 'TIMESHEET_QUERY_RECIPIENT_EMAIL_V1';
const IMPORT_REVIEW_CANONICAL_CORRECTION_CARRIER_CONTRACT =
  'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1';
const IMPORT_REVIEW_TARGETED_FAMILY_MATERIALISATION_CONTRACT =
  'BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_V1';

export const IMPORT_REVIEW_PARSER_VERSION = 'CLOUDTMS_IMPORT_REVIEW_PARSER_V1';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SHA256_RE = /^[0-9a-f]{64}$/;
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const MAX_BODY_BYTES = 262144;
const MAX_ACTION_CHANGES = 500;
const MAX_REVIEW_ACTIONS = 5000;

const ROUTE_RPCS = Object.freeze(new Set([
  'import_review_contract_version_get_v1',
  'import_review_list_v1',
  'import_review_staged_scope_get_v1',
  'import_review_create_v1',
  'import_review_replace_v1',
  'import_review_get_v1',
  'import_review_actions_page_v1',
  'import_review_save_v1',
  'import_review_refresh_v1',
  'import_review_abandon_v1',
  'hr_daily_timesheet_resolution_save_v1',
  'hr_weekly_candidate_not_worked_resolution_save_v1',
  'import_review_apply_status_get_v1',
  'import_review_apply_failed_before_commit_recover_v1',
  'nhsp_weekly_review_preview_v1',
  'hr_daily_validation_preview_v1',
  'nhsp_weekly_apply_transactional',
  'hr_weekly_apply_transactional',
  'hr_daily_apply_transactional'
]));

const READ_ONLY_ROUTE_RPCS = Object.freeze(new Set([
  'import_review_contract_version_get_v1',
  'import_review_list_v1',
  'import_review_staged_scope_get_v1',
  'import_review_get_v1',
  'import_review_actions_page_v1',
  'import_review_apply_status_get_v1',
  'nhsp_weekly_review_preview_v1',
  'hr_daily_validation_preview_v1'
]));

function isObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function jsonResponse(status, payload) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' }
  });
}

function success(data, status = 200, meta = null) {
  return jsonResponse(status, {
    ok: true,
    contract_version: IMPORT_REVIEW_DB_CONTRACT,
    data: data ?? null,
    ...(meta ? { meta } : {})
  });
}

function failure(status, code, message, options = {}) {
  const details = isObject(options.details) ? options.details : undefined;
  return jsonResponse(status, {
    ok: false,
    contract_version: IMPORT_REVIEW_DB_CONTRACT,
    error: {
      code,
      message,
      category: options.category || (status === 409 ? 'CONFLICT' : 'REQUEST'),
      retryable: options.retryable === true,
      ...(options.action ? { action: options.action } : {}),
      ...(details ? { details } : {})
    }
  });
}

function assertAllowedKeys(value, allowed, label = 'body') {
  if (!isObject(value)) throw new ImportReviewInputError(`${label} must be an object`);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length) {
    throw new ImportReviewInputError(`${label} contains unsupported fields`, { unsupported_fields: unknown.sort() });
  }
}

class ImportReviewInputError extends Error {
  constructor(message, details = null) {
    super(message);
    this.name = 'ImportReviewInputError';
    this.details = details;
  }
}

async function readBoundedJson(req) {
  const contentLength = Number(req.headers.get('content-length') || 0);
  if (Number.isFinite(contentLength) && contentLength > MAX_BODY_BYTES) {
    throw new ImportReviewInputError('Request body is too large');
  }
  const text = await req.text();
  if (new TextEncoder().encode(text).byteLength > MAX_BODY_BYTES) {
    throw new ImportReviewInputError('Request body is too large');
  }
  if (!text.trim()) return {};
  let parsed;
  try { parsed = JSON.parse(text); }
  catch { throw new ImportReviewInputError('Request body is not valid JSON'); }
  if (!isObject(parsed)) throw new ImportReviewInputError('Request body must be an object');
  return parsed;
}

function uuid(value, field, { nullable = false } = {}) {
  if (nullable && (value === null || value === undefined || value === '')) return null;
  const out = String(value || '').trim();
  if (!UUID_RE.test(out)) throw new ImportReviewInputError(`${field} must be a UUID`);
  return out.toLowerCase();
}

function boundedText(value, field, { min = 0, max = 256, nullable = false } = {}) {
  if (nullable && (value === null || value === undefined)) return null;
  const out = String(value ?? '').trim();
  if (out.length < min || out.length > max) {
    throw new ImportReviewInputError(`${field} must contain between ${min} and ${max} characters`);
  }
  return out;
}

function integer(value, field, min, max, fallback = null) {
  if ((value === null || value === undefined || value === '') && fallback !== null) return fallback;
  const out = Number(value);
  if (!Number.isInteger(out) || out < min || out > max) {
    throw new ImportReviewInputError(`${field} must be an integer between ${min} and ${max}`);
  }
  return out;
}

function isoDate(value, field, { nullable = false } = {}) {
  if (nullable && (value === null || value === undefined || value === '')) return null;
  const out = String(value || '').trim();
  if (!ISO_DATE_RE.test(out) || Number.isNaN(Date.parse(`${out}T00:00:00Z`))) {
    throw new ImportReviewInputError(`${field} must be YYYY-MM-DD`);
  }
  return out;
}

function sha256(value, field) {
  const out = String(value || '').trim().toLowerCase();
  if (!SHA256_RE.test(out)) throw new ImportReviewInputError(`${field} must be a lowercase SHA-256 value`);
  return out;
}

function actionIds(value, field, max = MAX_ACTION_CHANGES) {
  if (!Array.isArray(value) || value.length > max) {
    throw new ImportReviewInputError(`${field} must be an array containing at most ${max} action IDs`);
  }
  const out = value.map((entry) => sha256(entry, field));
  if (new Set(out).size !== out.length) throw new ImportReviewInputError(`${field} contains duplicate action IDs`);
  return out;
}

function rpcPayload(raw, name) {
  if (Array.isArray(raw) && raw.length === 1 && isObject(raw[0])) {
    if (Object.prototype.hasOwnProperty.call(raw[0], name)) return raw[0][name];
    return raw[0];
  }
  if (isObject(raw) && Object.prototype.hasOwnProperty.call(raw, name)) return raw[name];
  return raw;
}

function postgresErrorToken(error) {
  const parts = [
    error?.json?.message,
    error?.json?.details,
    error?.json?.hint,
    error?.body,
    error?.message
  ].filter(Boolean).map(String);
  const joined = parts.join(' | ').toUpperCase();
  const tokens = joined.match(/[A-Z][A-Z0-9_]{4,}/g) || [];
  return tokens.find((token) => token.startsWith('IMPORT_REVIEW_')
    || token.startsWith('HR_DAILY_')
    || token.startsWith('TIMESHEET_QUERY_')
    || token.startsWith('BLOCKED_')) || null;
}

function isPlpgsqlCheckCallStackFailure(error) {
  const status = Number(error?.status || 0);
  const code = String(error?.json?.code || error?.code || '').trim().toUpperCase();
  const message = [
    error?.json?.message,
    error?.json?.details,
    error?.body,
    error?.message
  ].filter(Boolean).map(String).join(' ').toLowerCase();
  return status >= 500
    && (!code || code === 'XX000')
    && message.includes('cannot find parent statement on pldbgapi2 call stack');
}

function mapRpcError(error) {
  if (error instanceof ImportReviewInputError) {
    return failure(400, 'IMPORT_REVIEW_INVALID_REQUEST', error.message, {
      category: 'VALIDATION',
      details: error.details
    });
  }

  const token = postgresErrorToken(error) || 'IMPORT_REVIEW_RPC_FAILED';
  if (/NOT_FOUND/.test(token)) {
    return failure(404, token, 'The import review or requested evidence was not found.', { category: 'NOT_FOUND' });
  }
  if (/VERSION_CONFLICT|PREVIEW_STALE|EVIDENCE_STALE|REVIEW_STALE|APPLY_STALE_OR_NOT_READY|SELECTED_ACTION_STALE|FOLLOW_UP_CONFLICT|REQUEST_CONFLICT|OPERATION_KEY_CONFLICT|REQUEST_MISMATCH|SOURCE_HASH_MISMATCH|PARSER_VERSION_MISMATCH|APPLY_REQUEST_HASH_MISMATCH|SELECTED_ACTION_SET_MISMATCH|RECIPIENT_ROUTE_CHANGED|ACTION_NOT_SELECTABLE_OR_STALE/.test(token)) {
    return failure(409, token, 'The review changed. Reload the review before continuing.', {
      category: 'STALE_CONFLICT',
      retryable: true,
      action: 'RELOAD_REVIEW'
    });
  }
  if (/ACTIVE_PAY_DRAFT|DRAFT_ACTIVE|PAY_DRAFT|REFERENCE_INVALIDATION_PROTECTED/.test(token)) {
    return failure(423, token, 'An active Banking Pay draft protects one or more selected timesheets.', {
      category: 'PROTECTED_ARTIFACT',
      action: 'LEAVE_IMPORT_AND_RESOLVE_BANKING_PAY'
    });
  }
  if (/NOT_ALLOWED|IMMUTABLE|ALREADY_APPLIED|APPLIED_IMMUTABLE|REQUIRED/.test(token)) {
    return failure(409, token, 'The requested action is not allowed in the review’s current state.', { category: 'STATE_CONFLICT' });
  }
  if (/INVALID|MISMATCH|OUTSIDE|LIMIT|TOO_LONG|UNKNOWN_FIELDS|REJECTED/.test(token)) {
    return failure(422, token, 'The request does not match the installed import-review contract.', { category: 'CONTRACT_VALIDATION' });
  }
  if (Number(error?.status) === 408 || /TIMEOUT|TIMED OUT|ABORT/.test(String(error?.message || '').toUpperCase())) {
    return failure(202, 'IMPORT_REVIEW_OUTCOME_UNKNOWN', 'The database outcome is not yet known. Check operation status before retrying.', {
      category: 'UNKNOWN_OUTCOME',
      retryable: true,
      action: 'CHECK_APPLY_STATUS'
    });
  }
  return failure(502, token, 'The import-review database operation failed safely.', {
    category: 'DATABASE',
    retryable: Number(error?.status) >= 500
  });
}

async function runAllowedRpc(sbRpc, env, name, args, options = {}) {
  if (!ROUTE_RPCS.has(name)) throw new Error(`Import-review RPC is not allowlisted: ${name}`);
  const canRetryRead = READ_ONLY_ROUTE_RPCS.has(name) && options.retryRead !== false;
  const maxAttempts = canRetryRead ? 2 : 1;
  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const raw = await sbRpc(env, name, args, { timeoutMs: options.timeoutMs || 15000 });
      return rpcPayload(raw, name);
    } catch (error) {
      lastError = error;
      const status = Number(error?.status || 0);
      const code = String(error?.code || error?.json?.code || '').trim().toUpperCase();
      const message = String(error?.message || '').toUpperCase();
      const transient = status === 408 || status === 500 || status === 502 || status === 503 || status === 504
        || /TIMEOUT|TIMED OUT|ECONNRESET|UPSTREAM/.test(`${code} ${message}`);
      if (attempt >= maxAttempts || !transient) throw error;
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }
  throw lastError;
}

async function assertContract(sbRpc, env) {
  let contract;
  try {
    contract = await runAllowedRpc(sbRpc, env, 'import_review_contract_version_get_v1', {}, { timeoutMs: 8000 });
  } catch (error) {
    const unavailable = new Error('IMPORT_REVIEW_CONTRACT_UNAVAILABLE');
    unavailable.cause = error;
    throw unavailable;
  }
  const valid = isObject(contract)
    && contract.ok === true
    && contract.schema_contract_version === IMPORT_REVIEW_DB_CONTRACT
    && contract.apply_envelope_version === IMPORT_REVIEW_APPLY_CONTRACT
    && contract.apply_operation_version === IMPORT_REVIEW_OPERATION_CONTRACT
    && contract.correction_operation_version === IMPORT_REVIEW_CORRECTION_CONTRACT
    && contract.follow_up_component_version === IMPORT_REVIEW_FOLLOW_UP_COMPONENT_CONTRACT
    && contract.tsfin_follow_up_settlement_version === IMPORT_REVIEW_TSFIN_SETTLEMENT_CONTRACT
    && contract.incremental_apply_version === IMPORT_REVIEW_INCREMENTAL_APPLY_CONTRACT
    && contract.review_ui_contract_version === IMPORT_REVIEW_UI_CONTRACT
    && contract.email_grouping_version === IMPORT_REVIEW_EMAIL_GROUPING_CONTRACT
    && contract.canonical_correction_carrier_version
      === IMPORT_REVIEW_CANONICAL_CORRECTION_CARRIER_CONTRACT
    && contract.targeted_family_materialisation_version
      === IMPORT_REVIEW_TARGETED_FAMILY_MATERIALISATION_CONTRACT
    && contract.legacy_contracts_supported === false;
  if (!valid) {
    const mismatch = new Error('IMPORT_REVIEW_CONTRACT_MISMATCH');
    mismatch.contract = contract;
    throw mismatch;
  }
  return contract;
}

function contractFailure(error) {
  const code = String(error?.message || '').includes('MISMATCH')
    ? 'IMPORT_REVIEW_CONTRACT_MISMATCH'
    : 'IMPORT_REVIEW_CONTRACT_UNAVAILABLE';
  return failure(503, code, 'Import review is temporarily unavailable because the Worker cannot prove the required database contract.', {
    category: 'CONTRACT_GATE',
    retryable: true,
    action: 'RETRY_LATER'
  });
}

function coverageScopeItems(value, field, keyName, max) {
  if (!Array.isArray(value) || value.length > max) {
    throw new ImportReviewInputError(`${field} must be an array containing at most ${max} items`);
  }
  const allowed = new Set([keyName, 'source_display_label', keyName === 'source_client_key' ? 'client_id' : 'candidate_id']);
  const ids = new Set();
  return value.map((entry) => {
    assertAllowedKeys(entry, allowed, field);
    const key = boundedText(entry[keyName], `${field}.${keyName}`, { min: 1, max: 512 });
    if (ids.has(key)) throw new ImportReviewInputError(`${field} contains duplicate source keys`);
    ids.add(key);
    const idField = keyName === 'source_client_key' ? 'client_id' : 'candidate_id';
    return {
      [keyName]: key,
      source_display_label: boundedText(entry.source_display_label, `${field}.source_display_label`, { max: 512, nullable: true }),
      [idField]: uuid(entry[idField], `${field}.${idField}`, { nullable: true })
    };
  });
}

function stateContractFromGet(review, operationId) {
  const state = review?.state;
  const imported = review?.import;
  const apply = state?.apply_contract;
  if (!isObject(state) || !isObject(imported) || !isObject(apply) || !isObject(apply.request_envelope)) {
    throw new ImportReviewInputError('The review response did not contain an apply contract');
  }
  return {
    review_contract: {
      schema_version: IMPORT_REVIEW_APPLY_CONTRACT,
      operation_id: operationId,
      state_version: state.state_version,
      coverage_fingerprint: imported.coverage_fingerprint,
      preview_fingerprint: state.preview_fingerprint,
      request_hash: apply.request_hash
    },
    review_selected_action_ids: actionIds(apply.selected_action_ids || [], 'selected_action_ids', MAX_REVIEW_ACTIONS),
    invalidation_action_ids: actionIds(apply.reference_invalidation_action_ids || [], 'invalidation_action_ids', MAX_REVIEW_ACTIONS)
  };
}

function sourceRouteToApplyRpc(route) {
  const upper = String(route || '').trim().toUpperCase();
  if (upper === 'NHSP') return 'nhsp_weekly_apply_transactional';
  if (upper === 'HR_WEEKLY' || upper === 'HEALTHROSTER' || upper === 'HEALTHROSTER_WEEKLY') return 'hr_weekly_apply_transactional';
  if (upper === 'HR_DAILY' || upper === 'HEALTHROSTER_DAILY') return 'hr_daily_apply_transactional';
  throw new ImportReviewInputError('The review source route is unsupported');
}

async function getReview(sbRpc, env, importId, actorId, options = {}) {
  return runAllowedRpc(sbRpc, env, 'import_review_get_v1', {
    p_import_id: importId,
    p_actor_user_id: actorId,
    p_action_cursor: options.actionCursor || null,
    p_action_limit: options.actionLimit || 100,
    p_event_cursor: options.eventCursor || null,
    p_event_limit: options.eventLimit || 50
  });
}

async function applyReview({ sbRpc, env, importId, actorId, body, runFollowUp, ctx }) {
  assertAllowedKeys(body, new Set(['operation_id', 'expected_state_version', 'expected_request_hash']));
  const operationId = uuid(body.operation_id, 'operation_id');
  const expectedStateVersion = integer(body.expected_state_version, 'expected_state_version', 1, Number.MAX_SAFE_INTEGER);
  const expectedRequestHash = sha256(body.expected_request_hash, 'expected_request_hash');

  const review = await getReview(sbRpc, env, importId, actorId, { actionLimit: 1, eventLimit: 1 });
  const dbStateVersion = Number(review?.state?.state_version);
  const dbRequestHash = String(review?.state?.apply_contract?.request_hash || '').toLowerCase();
  if (dbStateVersion !== expectedStateVersion || dbRequestHash !== expectedRequestHash) {
    return failure(409, 'IMPORT_REVIEW_APPLY_STALE', 'The review changed. Reload it before applying.', {
      category: 'STALE_CONFLICT',
      retryable: true,
      action: 'RELOAD_REVIEW'
    });
  }
  const allowedCommands = review?.state?.editability?.allowed_commands;
  if (!Array.isArray(allowedCommands) || !allowedCommands.includes('APPLY')) {
    return failure(409, 'IMPORT_REVIEW_NOT_READY', 'No selected candidate/client unit is currently ready to apply.', {
      category: 'STATE_CONFLICT',
      action: 'REVIEW_BLOCKERS'
    });
  }

  const payload = stateContractFromGet(review, operationId);
  const rpcName = sourceRouteToApplyRpc(review?.import?.source_route || review?.import?.source_system);
  const startFollowUp = (applyResult) => {
    if (typeof runFollowUp !== 'function') return false;
    const task = Promise.resolve().then(() => runFollowUp(env, {
      importId,
      operationId,
      actorUserId: actorId,
      requestHash: expectedRequestHash,
      applyResult
    }));
    if (ctx && typeof ctx.waitUntil === 'function') ctx.waitUntil(task);
    else return task;
    return true;
  };
  const readApplyStatus = () => runAllowedRpc(sbRpc, env, 'import_review_apply_status_get_v1', {
    p_import_id: importId,
    p_operation_id: operationId,
    p_request_hash: expectedRequestHash
  }, { timeoutMs: 8000 });
  const committedResponse = async (status, recoveryKind) => {
    if (!String(status?.outcome || '').startsWith('COMMITTED_')) return null;
    const recovered = status?.stored_response || {};
    const followUp = startFollowUp(recovered);
    if (followUp && typeof followUp.then === 'function') await followUp;
    return success({
      apply: recovered,
      operation: status,
      recovered_from_unknown_outcome: recoveryKind === 'UNKNOWN_OUTCOME',
      recovered_from_database_interruption: recoveryKind === 'PLPGSQL_CHECK',
      follow_up_started: followUp !== false
    });
  };
  let applied;
  let recoveredFromDatabaseInterruption = false;
  try {
    applied = await runAllowedRpc(sbRpc, env, rpcName, {
      p_import_id: importId,
      p_payload: payload,
      p_actor_user_id: actorId
    }, { timeoutMs: 30000 });
  } catch (error) {
    const timeout = Number(error?.status) === 408 || /TIMEOUT|TIMED OUT|ABORT/.test(String(error?.message || '').toUpperCase());
    const plpgsqlCheckFailure = isPlpgsqlCheckCallStackFailure(error);
    if (!timeout && !plpgsqlCheckFailure) throw error;
    try {
      const status = await readApplyStatus();
      const committed = await committedResponse(status, plpgsqlCheckFailure ? 'PLPGSQL_CHECK' : 'UNKNOWN_OUTCOME');
      if (committed) return committed;

      if (plpgsqlCheckFailure && String(status?.outcome || '').toUpperCase() === 'NOT_STARTED') {
        console.warn(JSON.stringify({
          event: 'IMPORT_REVIEW_PLPGSQL_CHECK_RECOVERY',
          rpc: rpcName,
          outcome: 'NOT_STARTED',
          retry_attempt: 1
        }));
        try {
          applied = await runAllowedRpc(sbRpc, env, rpcName, {
            p_import_id: importId,
            p_payload: payload,
            p_actor_user_id: actorId
          }, { timeoutMs: 30000 });
          recoveredFromDatabaseInterruption = true;
        } catch (retryError) {
          try {
            const retryStatus = await readApplyStatus();
            const retryCommitted = await committedResponse(retryStatus, 'PLPGSQL_CHECK');
            if (retryCommitted) return retryCommitted;
          } catch {}
          return failure(503, 'IMPORT_REVIEW_DATABASE_INTERRUPTED', 'A temporary database interruption prevented this application batch. CloudTMS verified that no source commit can be confirmed.', {
            category: 'DATABASE',
            retryable: true,
            action: 'CHECK_APPLY_STATUS'
          });
        }
      } else {
        return failure(202, 'IMPORT_REVIEW_OUTCOME_UNKNOWN', 'The database outcome is not yet known. Check operation status before retrying.', {
          category: 'UNKNOWN_OUTCOME', retryable: true, action: 'CHECK_APPLY_STATUS'
        });
      }
    } catch {
      return failure(202, 'IMPORT_REVIEW_OUTCOME_UNKNOWN', 'The database outcome is not yet known. Check operation status before retrying.', {
        category: 'UNKNOWN_OUTCOME', retryable: true, action: 'CHECK_APPLY_STATUS'
      });
    }
  }

  const followUp = startFollowUp(applied);
  if (followUp && typeof followUp.then === 'function') await followUp;
  return success({
    apply: applied,
    operation_id: operationId,
    recovered_from_database_interruption: recoveredFromDatabaseInterruption,
    follow_up_started: followUp !== false
  });
}

function parseListQuery(url) {
  const statusClass = boundedText(url.searchParams.get('status_class') || 'ACTIVE', 'status_class', { min: 1, max: 32 }).toUpperCase();
  if (!['ACTIVE', 'COMPLETED', 'ABANDONED', 'SUPERSEDED', 'ALL'].includes(statusClass)) {
    throw new ImportReviewInputError('status_class is invalid');
  }
  return {
    p_status_class: statusClass,
    p_source_route: boundedText(url.searchParams.get('source_route'), 'source_route', { max: 128, nullable: true }),
    p_client_id: uuid(url.searchParams.get('client_id'), 'client_id', { nullable: true }),
    p_date_from: isoDate(url.searchParams.get('date_from'), 'date_from', { nullable: true }),
    p_date_to: isoDate(url.searchParams.get('date_to'), 'date_to', { nullable: true }),
    p_cursor_updated_at: boundedText(url.searchParams.get('cursor_updated_at'), 'cursor_updated_at', { max: 64, nullable: true }),
    p_cursor_import_id: uuid(url.searchParams.get('cursor_import_id'), 'cursor_import_id', { nullable: true }),
    p_page_size: integer(url.searchParams.get('page_size'), 'page_size', 1, 100, 50)
  };
}

function allowedPageSize(value, field, fallback = 25) {
  const out = integer(value, field, 25, 100, fallback);
  if (![25, 50, 75, 100].includes(out)) {
    throw new ImportReviewInputError(`${field} must be 25, 50, 75 or 100`);
  }
  return out;
}

function parseActionPageQuery(url) {
  const sortBy = boundedText(url.searchParams.get('sort_by') || 'CANDIDATE', 'sort_by', { min: 1, max: 32 }).toUpperCase();
  const sortDirection = boundedText(url.searchParams.get('sort_direction') || 'ASC', 'sort_direction', { min: 1, max: 4 }).toUpperCase();
  const view = boundedText(url.searchParams.get('view') || 'ALL', 'view', { min: 1, max: 32 }).toUpperCase();
  if (!['CANDIDATE', 'CLIENT', 'WEEK_ENDING', 'WORK_DATE', 'ACTION', 'STATUS'].includes(sortBy)) {
    throw new ImportReviewInputError('sort_by is invalid');
  }
  if (!['ASC', 'DESC'].includes(sortDirection)) throw new ImportReviewInputError('sort_direction is invalid');
  if (![
    'ALL', 'PENDING', 'READY', 'EMAIL', 'NO_ACTION',
    'CONFIRM_STANDARD', 'CONFIRM_NON_STANDARD', 'CONFIRM_VALIDATION',
    'CONFIRM_EMAIL', 'CONFIRM_REFERENCE'
  ].includes(view)) {
    throw new ImportReviewInputError('view is invalid');
  }
  return {
    p_page_number: integer(url.searchParams.get('page'), 'page', 1, 10000, 1),
    p_page_size: allowedPageSize(url.searchParams.get('page_size'), 'page_size', 25),
    p_sort_by: sortBy,
    p_sort_direction: sortDirection,
    p_view: view
  };
}

function sourceKeys(items, keyName) {
  return (Array.isArray(items) ? items : []).map((item) => String(item?.[keyName] || '').trim()).filter(Boolean);
}

function sameStringSet(left, right) {
  const a = [...new Set(left)].sort();
  const b = [...new Set(right)].sort();
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

function routeMatch(pathname, pattern) {
  const actual = pathname.split('/').filter(Boolean);
  const expected = pattern.split('/').filter(Boolean);
  if (actual.length !== expected.length) return null;
  const params = {};
  for (let i = 0; i < expected.length; i += 1) {
    if (expected[i].startsWith(':')) params[expected[i].slice(1)] = decodeURIComponent(actual[i]);
    else if (actual[i] !== expected[i]) return null;
  }
  return params;
}

export function createImportReviewDispatcher(dependencies) {
  const { requireUser, sbRpc, runFollowUp } = dependencies || {};
  if (typeof requireUser !== 'function' || typeof sbRpc !== 'function') {
    throw new Error('createImportReviewDispatcher requires requireUser and sbRpc');
  }

  return async function dispatchImportReview(req, env, ctx, pathname = new URL(req.url).pathname) {
    const isNewRoute = pathname === '/api/import-review/contract'
      || pathname === '/api/import-reviews'
      || pathname.startsWith('/api/import-reviews/');
    if (!isNewRoute) return null;

    const user = await requireUser(env, req, ['admin']);
    if (!user) return failure(401, 'AUTHENTICATION_REQUIRED', 'Sign in as an administrator to use import review.', { category: 'AUTH' });

    let contract;
    try { contract = await assertContract(sbRpc, env); }
    catch (error) { return contractFailure(error); }

    if (req.method === 'GET' && pathname === '/api/import-review/contract') return success(contract);

    try {
      const url = new URL(req.url);
      if (req.method === 'GET' && pathname === '/api/import-reviews') {
        const data = await runAllowedRpc(sbRpc, env, 'import_review_list_v1', parseListQuery(url));
        return success(data);
      }

      const stagedScope = routeMatch(pathname, '/api/import-reviews/staged/:import_id/scope');
      if (req.method === 'GET' && stagedScope) {
        const data = await runAllowedRpc(sbRpc, env, 'import_review_staged_scope_get_v1', {
          p_import_id: uuid(stagedScope.import_id, 'import_id'),
          p_actor_user_id: user.id,
          p_candidate_page: integer(url.searchParams.get('candidate_page'), 'candidate_page', 1, 20, 1),
          p_candidate_page_size: allowedPageSize(url.searchParams.get('candidate_page_size'), 'candidate_page_size', 100)
        });
        return success(data);
      }

      if (req.method === 'POST' && pathname === '/api/import-reviews') {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set([
          'import_id', 'coverage_mode', 'coverage_start_date', 'coverage_end_date',
          'scope_clients', 'scope_candidates', 'expected_source_file_sha256',
          'expected_parser_version', 'operation_key',
          'supersede_import_id', 'expected_supersede_state_version'
        ]));
        const mode = boundedText(body.coverage_mode, 'coverage_mode', { min: 1, max: 64 }).toUpperCase();
        if (!['COMPLETE_ALL', 'COMPLETE_SELECTED_CANDIDATES', 'PARTIAL'].includes(mode)) {
          throw new ImportReviewInputError('coverage_mode is invalid');
        }
        const importId = uuid(body.import_id, 'import_id');
        const staged = await runAllowedRpc(sbRpc, env, 'import_review_staged_scope_get_v1', {
          p_import_id: importId,
          p_actor_user_id: user.id,
          p_candidate_page: 1,
          p_candidate_page_size: 500
        });
        if (!isObject(staged) || staged.candidate_has_next === true) {
          throw new ImportReviewInputError('The staged import scope could not be proven within the configured bound');
        }
        const stagedClients = Array.isArray(staged.scope_clients) ? staged.scope_clients : [];
        const stagedCandidates = Array.isArray(staged.candidate_options) ? staged.candidate_options : [];
        const submittedClients = coverageScopeItems(body.scope_clients || [], 'scope_clients', 'source_client_key', 100);
        const submittedCandidates = coverageScopeItems(body.scope_candidates || [], 'scope_candidates', 'source_candidate_key', 500);
        const expectedStart = isoDate(body.coverage_start_date, 'coverage_start_date');
        const expectedEnd = isoDate(body.coverage_end_date, 'coverage_end_date');
        if (expectedStart !== String(staged.coverage_start_date || '') || expectedEnd !== String(staged.coverage_end_date || '')) {
          throw new ImportReviewInputError('Coverage dates must match the server-owned staged file date range');
        }
        const stagedClientKeys = sourceKeys(stagedClients, 'source_client_key');
        if (submittedClients.length && !sameStringSet(sourceKeys(submittedClients, 'source_client_key'), stagedClientKeys)) {
          throw new ImportReviewInputError('scope_clients must match the server-owned staged client scope');
        }
        const candidateByKey = new Map(stagedCandidates.map((item) => [String(item?.source_candidate_key || '').trim(), item]));
        const submittedCandidateKeys = sourceKeys(submittedCandidates, 'source_candidate_key');
        if (submittedCandidateKeys.some((key) => !candidateByKey.has(key))) {
          throw new ImportReviewInputError('scope_candidates contains a candidate outside the staged file');
        }
        if (mode === 'COMPLETE_SELECTED_CANDIDATES' && submittedCandidateKeys.length === 0) {
          throw new ImportReviewInputError('scope_candidates is required for COMPLETE_SELECTED_CANDIDATES');
        }
        if (mode !== 'COMPLETE_SELECTED_CANDIDATES' && submittedCandidateKeys.length > 0) {
          throw new ImportReviewInputError('scope_candidates is only allowed for COMPLETE_SELECTED_CANDIDATES');
        }
        const serverCandidates = submittedCandidateKeys.map((key) => {
          const item = candidateByKey.get(key) || {};
          return {
            source_candidate_key: key,
            source_display_label: item.source_display_label == null ? null : String(item.source_display_label),
            candidate_id: item.candidate_id || null
          };
        });
        const replaceRequested = body.supersede_import_id != null || body.expected_supersede_state_version != null;
        if (replaceRequested && (body.supersede_import_id == null || body.expected_supersede_state_version == null)) {
          throw new ImportReviewInputError('supersede_import_id and expected_supersede_state_version must be supplied together');
        }
        const rpcName = replaceRequested ? 'import_review_replace_v1' : 'import_review_create_v1';
        const rpcArgs = {
          p_import_id: importId,
          p_coverage_mode: mode,
          p_coverage_start_date: expectedStart,
          p_coverage_end_date: expectedEnd,
          p_scope_clients: stagedClients.map((item) => ({
            source_client_key: String(item.source_client_key || '').trim(),
            source_display_label: item.source_display_label == null ? null : String(item.source_display_label),
            client_id: item.client_id || null
          })),
          p_scope_candidates: serverCandidates,
          p_expected_source_file_sha256: sha256(body.expected_source_file_sha256, 'expected_source_file_sha256'),
          p_expected_parser_version: boundedText(body.expected_parser_version, 'expected_parser_version', { min: 1, max: 128 }),
          p_actor_user_id: user.id,
          p_operation_key: boundedText(body.operation_key, 'operation_key', { min: 16, max: 256 })
        };
        if (replaceRequested) {
          rpcArgs.p_supersede_import_id = uuid(body.supersede_import_id, 'supersede_import_id');
          rpcArgs.p_expected_supersede_state_version = integer(
            body.expected_supersede_state_version,
            'expected_supersede_state_version',
            1,
            Number.MAX_SAFE_INTEGER
          );
        }
        const data = await runAllowedRpc(sbRpc, env, rpcName, rpcArgs, { timeoutMs: 30000 });
        return success(data, 201);
      }

      const base = routeMatch(pathname, '/api/import-reviews/:import_id');
      if (req.method === 'GET' && base) {
        const importId = uuid(base.import_id, 'import_id');
        const data = await getReview(sbRpc, env, importId, user.id, {
          actionCursor: url.searchParams.get('action_cursor') || null,
          actionLimit: integer(url.searchParams.get('action_limit'), 'action_limit', 1, 200, 100),
          eventCursor: url.searchParams.get('event_cursor') == null ? null : integer(url.searchParams.get('event_cursor'), 'event_cursor', 0, Number.MAX_SAFE_INTEGER),
          eventLimit: integer(url.searchParams.get('event_limit'), 'event_limit', 1, 100, 50)
        });
        return success(data);
      }

      const actionPage = routeMatch(pathname, '/api/import-reviews/:import_id/actions');
      if (req.method === 'GET' && actionPage) {
        const data = await runAllowedRpc(sbRpc, env, 'import_review_actions_page_v1', {
          p_import_id: uuid(actionPage.import_id, 'import_id'),
          p_actor_user_id: user.id,
          ...parseActionPageQuery(url)
        });
        return success(data);
      }

      const save = routeMatch(pathname, '/api/import-reviews/:import_id/selections');
      if (req.method === 'PUT' && save) {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set(['expected_state_version', 'expected_preview_generation', 'expected_preview_fingerprint', 'action_changes', 'ui_state', 'request_id']));
        if (!Array.isArray(body.action_changes) || body.action_changes.length > MAX_ACTION_CHANGES) {
          throw new ImportReviewInputError(`action_changes must contain at most ${MAX_ACTION_CHANGES} items`);
        }
        const changes = body.action_changes.map((item) => {
          assertAllowedKeys(item, new Set(['action_id', 'selected']), 'action_changes item');
          if (typeof item.selected !== 'boolean') throw new ImportReviewInputError('action_changes.selected must be boolean');
          return { action_id: sha256(item.action_id, 'action_id'), selected: item.selected };
        });
        const data = await runAllowedRpc(sbRpc, env, 'import_review_save_v1', {
          p_import_id: uuid(save.import_id, 'import_id'),
          p_expected_state_version: integer(body.expected_state_version, 'expected_state_version', 1, Number.MAX_SAFE_INTEGER),
          p_expected_preview_generation: integer(body.expected_preview_generation, 'expected_preview_generation', 1, Number.MAX_SAFE_INTEGER),
          p_expected_preview_fingerprint: sha256(body.expected_preview_fingerprint, 'expected_preview_fingerprint'),
          p_action_changes: changes,
          p_ui_state_json: body.ui_state == null ? null : body.ui_state,
          p_actor_user_id: user.id,
          p_request_id: uuid(body.request_id, 'request_id')
        });
        return success(data);
      }

      const refresh = routeMatch(pathname, '/api/import-reviews/:import_id/refresh');
      if (req.method === 'POST' && refresh) {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set(['expected_state_version', 'max_actions']));
        const refreshArgs = {
          p_import_id: uuid(refresh.import_id, 'import_id'),
          p_expected_state_version: integer(body.expected_state_version, 'expected_state_version', 1, Number.MAX_SAFE_INTEGER),
          p_actor_user_id: user.id,
          p_max_actions: integer(body.max_actions, 'max_actions', 1, MAX_REVIEW_ACTIONS, MAX_REVIEW_ACTIONS)
        };
        let data;
        try {
          data = await runAllowedRpc(sbRpc, env, 'import_review_refresh_v1', refreshArgs, { timeoutMs: 30000 });
        } catch (error) {
          if (!isPlpgsqlCheckCallStackFailure(error)) throw error;
          console.warn(JSON.stringify({
            event: 'IMPORT_REVIEW_PLPGSQL_CHECK_RECOVERY',
            rpc: 'import_review_refresh_v1',
            outcome: 'TRANSACTION_ABORTED',
            retry_attempt: 1
          }));
          data = await runAllowedRpc(sbRpc, env, 'import_review_refresh_v1', refreshArgs, { timeoutMs: 30000 });
        }
        return success(data);
      }

      const abandon = routeMatch(pathname, '/api/import-reviews/:import_id/abandon');
      if (req.method === 'POST' && abandon) {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set(['expected_state_version', 'reason', 'confirmed']));
        if (body.confirmed !== true) throw new ImportReviewInputError('confirmed must be true to abandon an import');
        const data = await runAllowedRpc(sbRpc, env, 'import_review_abandon_v1', {
          p_import_id: uuid(abandon.import_id, 'import_id'),
          p_expected_state_version: integer(body.expected_state_version, 'expected_state_version', 1, Number.MAX_SAFE_INTEGER),
          p_reason: boundedText(body.reason, 'reason', { min: 1, max: 500 }),
          p_actor_user_id: user.id
        });
        return success(data);
      }

      const supersede = routeMatch(pathname, '/api/import-reviews/:import_id/supersede');
      if (req.method === 'POST' && supersede) {
        return jsonResponse(410, {
          ok: false,
          error: 'IMPORT_REVIEW_SUPERSEDE_ROUTE_RETIRED',
          message: 'Use atomic replacement while creating the new review.'
        });
      }

      const dailyResolution = routeMatch(pathname, '/api/import-reviews/:import_id/daily-timesheet-resolution');
      if (req.method === 'PUT' && dailyResolution) {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set(['hr_row_id', 'timesheet_id', 'expected_state_version', 'expected_preview_generation', 'expected_evidence_fingerprint', 'request_id']));
        const data = await runAllowedRpc(sbRpc, env, 'hr_daily_timesheet_resolution_save_v1', {
          p_import_id: uuid(dailyResolution.import_id, 'import_id'),
          p_hr_row_id: uuid(body.hr_row_id, 'hr_row_id'),
          p_timesheet_id: uuid(body.timesheet_id, 'timesheet_id', { nullable: true }),
          p_expected_state_version: integer(body.expected_state_version, 'expected_state_version', 1, Number.MAX_SAFE_INTEGER),
          p_expected_preview_generation: integer(body.expected_preview_generation, 'expected_preview_generation', 1, Number.MAX_SAFE_INTEGER),
          p_expected_evidence_fingerprint: sha256(body.expected_evidence_fingerprint, 'expected_evidence_fingerprint'),
          p_actor_user_id: user.id,
          p_request_id: uuid(body.request_id, 'request_id')
        });
        return success(data);
      }

      const weeklyCandidateNotWorked = routeMatch(pathname, '/api/import-reviews/:import_id/weekly-candidate-not-worked');
      if (req.method === 'PUT' && weeklyCandidateNotWorked) {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set([
          'action_id', 'confirmed', 'expected_state_version', 'expected_preview_generation',
          'expected_evidence_fingerprint', 'request_id'
        ]));
        if (typeof body.confirmed !== 'boolean') {
          throw new ImportReviewInputError('confirmed must be boolean');
        }
        const data = await runAllowedRpc(sbRpc, env, 'hr_weekly_candidate_not_worked_resolution_save_v1', {
          p_import_id: uuid(weeklyCandidateNotWorked.import_id, 'import_id'),
          p_action_id: sha256(body.action_id, 'action_id'),
          p_confirmed: body.confirmed,
          p_expected_state_version: integer(body.expected_state_version, 'expected_state_version', 1, Number.MAX_SAFE_INTEGER),
          p_expected_preview_generation: integer(body.expected_preview_generation, 'expected_preview_generation', 1, Number.MAX_SAFE_INTEGER),
          p_expected_evidence_fingerprint: sha256(body.expected_evidence_fingerprint, 'expected_evidence_fingerprint'),
          p_actor_user_id: user.id,
          p_request_id: uuid(body.request_id, 'request_id')
        });
        return success(data);
      }

      const preview = routeMatch(pathname, '/api/import-reviews/:import_id/preview');
      if (req.method === 'GET' && preview) {
        const importId = uuid(preview.import_id, 'import_id');
        const route = boundedText(url.searchParams.get('source_route'), 'source_route', { min: 1, max: 64 }).toUpperCase();
        const after = url.searchParams.get('after_action_id');
        const limit = integer(url.searchParams.get('limit'), 'limit', 1, 200, 100);
        if (route === 'NHSP') {
          return success(await runAllowedRpc(sbRpc, env, 'nhsp_weekly_review_preview_v1', {
            p_import_id: importId, p_after_action_id: after ? sha256(after, 'after_action_id') : null, p_limit: limit
          }));
        }
        if (route === 'HR_DAILY' || route === 'HEALTHROSTER_DAILY') {
          return success(await runAllowedRpc(sbRpc, env, 'hr_daily_validation_preview_v1', {
            p_import_id: importId, p_after_action_id: after ? sha256(after, 'after_action_id') : null, p_limit: limit
          }));
        }
        if (route === 'HR_WEEKLY' || route === 'HEALTHROSTER' || route === 'HEALTHROSTER_WEEKLY') {
          return success(await getReview(sbRpc, env, importId, user.id, { actionCursor: after || null, actionLimit: limit, eventLimit: 1 }));
        }
        throw new ImportReviewInputError('source_route is invalid');
      }

      const apply = routeMatch(pathname, '/api/import-reviews/:import_id/apply');
      if (req.method === 'POST' && apply) {
        const body = await readBoundedJson(req);
        return await applyReview({
          sbRpc, env, importId: uuid(apply.import_id, 'import_id'), actorId: user.id,
          body, runFollowUp, ctx
        });
      }

      const status = routeMatch(pathname, '/api/import-reviews/:import_id/apply-status');
      if (req.method === 'GET' && status) {
        const data = await runAllowedRpc(sbRpc, env, 'import_review_apply_status_get_v1', {
          p_import_id: uuid(status.import_id, 'import_id'),
          p_operation_id: uuid(url.searchParams.get('operation_id'), 'operation_id'),
          p_request_hash: sha256(url.searchParams.get('request_hash'), 'request_hash')
        }, { timeoutMs: 8000 });
        if (data?.ok === false || String(data?.status || '').toUpperCase() === 'OPERATION_REQUEST_MISMATCH') {
          return failure(409, 'IMPORT_REVIEW_OPERATION_REQUEST_MISMATCH', 'The operation ID belongs to a different request.', {
            category: 'STALE_CONFLICT', action: 'RELOAD_REVIEW'
          });
        }
        return success(data);
      }

      const recover = routeMatch(pathname, '/api/import-reviews/:import_id/apply-recover');
      if (req.method === 'POST' && recover) {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set(['operation_id', 'request_hash']));
        const data = await runAllowedRpc(sbRpc, env, 'import_review_apply_failed_before_commit_recover_v1', {
          p_import_id: uuid(recover.import_id, 'import_id'),
          p_operation_id: uuid(body.operation_id, 'operation_id'),
          p_request_hash: sha256(body.request_hash, 'request_hash'),
          p_actor_user_id: user.id
        }, { timeoutMs: 8000 });
        return success(data);
      }

      const retry = routeMatch(pathname, '/api/import-reviews/:import_id/follow-up/retry');
      if (req.method === 'POST' && retry) {
        const body = await readBoundedJson(req);
        assertAllowedKeys(body, new Set(['operation_id', 'request_hash']));
        const importId = uuid(retry.import_id, 'import_id');
        const operationId = uuid(body.operation_id, 'operation_id');
        const requestHash = sha256(body.request_hash, 'request_hash');
        const statusData = await runAllowedRpc(sbRpc, env, 'import_review_apply_status_get_v1', {
          p_import_id: importId, p_operation_id: operationId, p_request_hash: requestHash
        }, { timeoutMs: 8000 });
        if (!String(statusData?.outcome || '').startsWith('COMMITTED_')) {
          return failure(409, 'IMPORT_REVIEW_FOLLOW_UP_NOT_COMMITTED', 'Source apply has not been proven committed.', {
            category: 'STATE_CONFLICT', action: 'CHECK_APPLY_STATUS'
          });
        }
        const followUpStatus = String(statusData?.follow_up_status || '').trim().toUpperCase();
        if (followUpStatus === 'COMPLETE' || followUpStatus === 'NOT_REQUIRED') {
          return success({
            operation_id: operationId,
            follow_up_started: false,
            follow_up_status: followUpStatus,
            no_action_required: true
          });
        }
        if (followUpStatus === 'PENDING') {
          return failure(409, 'IMPORT_REVIEW_FOLLOW_UP_ALREADY_PENDING', 'Follow-up is already in progress. Refresh its status before retrying.', {
            category: 'STATE_CONFLICT', retryable: true, action: 'REFRESH_APPLY_STATUS'
          });
        }
        if (followUpStatus !== 'FAILED_RETRYABLE') {
          return failure(409, 'IMPORT_REVIEW_FOLLOW_UP_STATE_INVALID', 'Follow-up is not in a retryable state.', {
            category: 'STATE_CONFLICT', action: 'REFRESH_APPLY_STATUS'
          });
        }
        if (typeof runFollowUp !== 'function') {
          return failure(503, 'IMPORT_REVIEW_FOLLOW_UP_UNAVAILABLE', 'The follow-up worker is unavailable.', { category: 'WORKER', retryable: true });
        }
        const task = Promise.resolve().then(() => runFollowUp(env, {
          importId,
          operationId,
          actorUserId: user.id,
          requestHash,
          applyResult: statusData.stored_response || {}
        }));
        if (ctx && typeof ctx.waitUntil === 'function') ctx.waitUntil(task);
        else await task;
        return success({ operation_id: operationId, follow_up_started: true }, 202);
      }

      return failure(404, 'IMPORT_REVIEW_ROUTE_NOT_FOUND', 'The import-review route does not exist.', { category: 'NOT_FOUND' });
    } catch (error) {
      return mapRpcError(error);
    }
  };
}

export async function importReviewSourceEvidenceFromR2(env, fileKey, parserVersion = IMPORT_REVIEW_PARSER_VERSION) {
  const key = boundedText(fileKey, 'file_key', { min: 1, max: 2048 });
  const bucket = env.R2_BUCKET || env.R2;
  if (!bucket || typeof bucket.get !== 'function') throw new Error('IMPORT_SOURCE_STORAGE_UNAVAILABLE');
  const object = await bucket.get(key);
  if (!object) throw new Error('IMPORT_SOURCE_FILE_NOT_FOUND');
  const configuredMax = Number.parseInt(String(env.FILE_MAX_BYTES || '5000000'), 10);
  const maxBytes = Number.isInteger(configuredMax) && configuredMax > 0
    ? Math.min(configuredMax, 25 * 1024 * 1024)
    : 5000000;
  if (Number(object.size || 0) > maxBytes) throw new Error('IMPORT_SOURCE_FILE_TOO_LARGE');
  const bytes = await object.arrayBuffer();
  if (bytes.byteLength > maxBytes) throw new Error('IMPORT_SOURCE_FILE_TOO_LARGE');
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  const sourceFileSha256 = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
  return {
    source_file_sha256: sourceFileSha256,
    parser_version: boundedText(parserVersion, 'parser_version', { min: 1, max: 128 }),
    source_file_size_bytes: bytes.byteLength
  };
}

export function normalizeContractQueryEmailOverride(input, current = {}) {
  const source = isObject(input) ? input : {};
  const hasEnabled = Object.prototype.hasOwnProperty.call(source, 'send_ts_queries_to_different_email');
  const hasAddress = Object.prototype.hasOwnProperty.call(source, 'ts_queries_alt_email_address');
  const enabled = hasEnabled ? source.send_ts_queries_to_different_email === true : current.send_ts_queries_to_different_email === true;
  const rawAddress = hasAddress ? source.ts_queries_alt_email_address : current.ts_queries_alt_email_address;
  const address = rawAddress == null ? null : String(rawAddress).trim().toLowerCase() || null;
  if (address && (address.length > 320 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(address))) {
    throw new ImportReviewInputError('ts_queries_alt_email_address must be a valid email address');
  }
  if (enabled && !address) {
    throw new ImportReviewInputError('ts_queries_alt_email_address is required when the contract override is enabled');
  }
  return {
    send_ts_queries_to_different_email: enabled,
    ts_queries_alt_email_address: enabled ? address : null
  };
}

export const importReviewContract = Object.freeze({
  schema: IMPORT_REVIEW_DB_CONTRACT,
  apply: IMPORT_REVIEW_APPLY_CONTRACT,
  operation: IMPORT_REVIEW_OPERATION_CONTRACT,
  correction: IMPORT_REVIEW_CORRECTION_CONTRACT,
  followUpComponent: IMPORT_REVIEW_FOLLOW_UP_COMPONENT_CONTRACT,
  tsfinSettlement: IMPORT_REVIEW_TSFIN_SETTLEMENT_CONTRACT,
  incrementalApply: IMPORT_REVIEW_INCREMENTAL_APPLY_CONTRACT,
  reviewUi: IMPORT_REVIEW_UI_CONTRACT,
  emailGrouping: IMPORT_REVIEW_EMAIL_GROUPING_CONTRACT,
  canonicalCorrectionCarrier:
    IMPORT_REVIEW_CANONICAL_CORRECTION_CARRIER_CONTRACT,
  targetedFamilyMaterialisation:
    IMPORT_REVIEW_TARGETED_FAMILY_MATERIALISATION_CONTRACT
});
