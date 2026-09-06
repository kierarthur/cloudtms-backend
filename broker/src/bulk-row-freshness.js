const SURFACES = new Set(['bulk_process', 'bulk_authorise']);
const CLASSIFICATIONS = new Set(['TIMESHEETS', 'NHSP', 'HR']);
const PROCESS_SECTIONS = new Set(['unprocessed_eligible', 'processed_eligible']);
const AUTHORISE_VISIBLE_SECTIONS = new Set([
  'processed_eligible',
  'authorised_eligible',
  'processed_review_required'
]);

const FILTER_KEYS = [
  'q',
  'candidate_id',
  'client_id',
  'date_from',
  'date_to',
  'week_ending_date',
  'week_ending_from',
  'week_ending_to',
  'bucket',
  'show_weekly_manual',
  'show_daily_manual',
  'show_daily',
  'show_weekly',
  'show_manual',
  'show_qr',
  'show_electronic',
  'validation_already',
  'validation_awaiting',
  'show_authorised_invoiced_unissued'
];

const trim = (value) => String(value == null ? '' : value).trim();
const upper = (value) => trim(value).toUpperCase();
const boolish = (value) => {
  if (value === true || value === 1) return true;
  if (value === false || value == null) return false;
  return ['true', '1', 'yes', 'y', 'on'].includes(trim(value).toLowerCase());
};
const plainObject = (value) => value && typeof value === 'object' && !Array.isArray(value) ? value : {};

function firstParam(params, ...keys) {
  for (const key of keys) {
    const value = params.get(key);
    if (value != null) return value;
  }
  return null;
}

export function buildBulkRowFreshnessRequest(urlInput, actorUserId = null) {
  const url = urlInput instanceof URL ? urlInput : new URL(String(urlInput));
  const params = url.searchParams;
  const surface = trim(firstParam(params, 'surface')).toLowerCase();
  const classificationRaw = upper(firstParam(params, 'classification'));
  const classification = CLASSIFICATIONS.has(classificationRaw) ? classificationRaw : null;
  const rowKey = trim(firstParam(params, 'row_key', 'rowKey'));
  const previousRowKey = trim(firstParam(params, 'previous_row_key', 'previousRowKey'));
  const timesheetId = trim(firstParam(params, 'timesheet_id', 'timesheetId', 'current_timesheet_id', 'currentTimesheetId'));
  const contractWeekId = trim(firstParam(params, 'contract_week_id', 'contractWeekId'));
  const knownSignature = trim(firstParam(params, 'known_signature', 'knownSignature'));
  const currentSection = trim(firstParam(params, 'current_section', 'currentSection')).toLowerCase();

  if (!SURFACES.has(surface)) {
    throw Object.assign(new Error('surface must be bulk_process or bulk_authorise'), { code: 'INVALID_SURFACE' });
  }
  if (!rowKey && !previousRowKey && !timesheetId && !contractWeekId) {
    throw Object.assign(new Error('At least one row identity is required'), { code: 'ROW_IDENTITY_REQUIRED' });
  }
  if (surface === 'bulk_authorise' && !classification) {
    throw Object.assign(new Error('A valid Bulk Authorise classification is required'), { code: 'INVALID_CLASSIFICATION' });
  }

  const filters = {};
  for (const key of FILTER_KEYS) {
    const value = params.get(key);
    if (value != null && trim(value) !== '') filters[key] = value;
  }
  if (classification) filters.classification = classification;

  return {
    surface,
    classification,
    row_key: rowKey || null,
    previous_row_key: previousRowKey || null,
    timesheet_id: timesheetId || null,
    contract_week_id: contractWeekId || null,
    known_signature: knownSignature || null,
    current_section: currentSection || null,
    actor_user_id: trim(actorUserId) || null,
    filters
  };
}

export function buildBulkRowResolutionAttempts(request) {
  const mode = request.surface === 'bulk_authorise' ? 'authorise' : 'process';
  const base = { dataset_mode: mode, projection: 'status_patch', profile: 'list' };
  if (request.actor_user_id) base.actor_user_id = request.actor_user_id;
  const attempts = [];
  const rowKeys = Array.from(new Set([request.row_key, request.previous_row_key].map(trim).filter(Boolean)));
  if (rowKeys.length) attempts.push({ ...base, row_keys: rowKeys });
  if (request.contract_week_id) attempts.push({ ...base, contract_week_id: request.contract_week_id });
  if (request.timesheet_id) attempts.push({ ...base, timesheet_id: request.timesheet_id });
  return attempts;
}

export function unwrapBulkRowPatch(raw) {
  let value = raw;
  if (Array.isArray(value)) value = value[0];
  if (plainObject(value).row_json) value = value.row_json;
  if (plainObject(value).bulk_timesheet_row_patch_v1) value = value.bulk_timesheet_row_patch_v1;
  if (typeof value === 'string') {
    try { value = JSON.parse(value); } catch { value = null; }
  }
  return plainObject(value);
}

function canonicalIdentity(row) {
  const rowKey = trim(row.row_key || row.new_row_key);
  const timesheetId = trim(row.current_timesheet_id || row.timesheet_id || row.requested_timesheet_id);
  const keyWeekId = /^contract_week:/i.test(rowKey) ? trim(rowKey.replace(/^contract_week:/i, '')) : '';
  const contractWeekId = trim(row.contract_week_id || row.contractWeekId || keyWeekId);
  return { rowKey, timesheetId, contractWeekId };
}

function sameLogicalRow(row, identity) {
  if (!row || typeof row !== 'object') return false;
  const rowIdentity = canonicalIdentity(row);
  if (identity.rowKey && rowIdentity.rowKey === identity.rowKey) return true;
  if (identity.contractWeekId && rowIdentity.contractWeekId === identity.contractWeekId) return true;
  if (identity.timesheetId && rowIdentity.timesheetId === identity.timesheetId) return true;
  return false;
}

function datasetFilters(request, identity) {
  const filters = { ...plainObject(request.filters), limit: 2, offset: 0, profile: 'list', projection: 'dataset_row' };
  delete filters.bucket;
  if (identity.rowKey) filters.row_key = identity.rowKey;
  if (identity.timesheetId) filters.timesheet_id = identity.timesheetId;
  if (identity.contractWeekId) filters.contract_week_id = identity.contractWeekId;
  if (request.actor_user_id) filters.actor_user_id = request.actor_user_id;
  return filters;
}

function membershipFromDataset(surface, raw, identity) {
  let payload = raw;
  if (Array.isArray(payload) && payload.length === 1) payload = payload[0];
  const expectedKey = surface === 'bulk_process' ? 'bulk_process_dataset_v1' : 'bulk_authorise_dataset_v1';
  if (plainObject(payload)[expectedKey]) payload = payload[expectedKey];
  payload = plainObject(payload);

  if (surface === 'bulk_process') {
    for (const [arrayKey, section] of [['unprocessed_rows', 'unprocessed_eligible'], ['processed_rows', 'processed_eligible']]) {
      const row = (Array.isArray(payload[arrayKey]) ? payload[arrayKey] : []).find((candidate) => sameLogicalRow(candidate, identity));
      if (row) return { row, section };
    }
    return { row: null, section: null };
  }

  const row = (Array.isArray(payload.rows) ? payload.rows : []).find((candidate) => sameLogicalRow(candidate, identity));
  const section = trim(row?.bulk_authorise_section).toLowerCase();
  return row && AUTHORISE_VISIBLE_SECTIONS.has(section) ? { row, section } : { row: null, section: null };
}

function removalReason(surface, patch) {
  const archived = boolish(patch.is_archived) || !!trim(patch.archived_at_utc) || upper(patch.stage || patch.tools_stage || patch.summary_stage) === 'ARCHIVED';
  if (archived) return 'ARCHIVED';
  const authorised = boolish(patch.is_authorised) || upper(patch.processing_status || patch.status) === 'AUTHORISED';
  const bucket = upper(patch.bulk_process_bucket || patch.processing_status || patch.status);
  if (surface === 'bulk_process' && authorised) return 'AUTHORISED_OUT_OF_BULK_PROCESS';
  if (surface === 'bulk_authorise' && (bucket === 'UNPROCESSED' || boolish(patch.is_unprocessed))) return 'UNPROCESSED_OUT_OF_BULK_AUTHORISE';
  return 'FILTERED_OUT';
}

export async function resolveBulkRowFreshness(request, rpc) {
  if (typeof rpc !== 'function') throw new TypeError('rpc is required');
  const deadline = Date.now() + 4500;
  const boundedTimeout = (preferredMs) => {
    const remaining = deadline - Date.now();
    if (remaining <= 250) {
      throw Object.assign(new Error('Bulk row freshness timed out'), { status: 408 });
    }
    return Math.max(250, Math.min(preferredMs, remaining - 100));
  };
  const attempts = buildBulkRowResolutionAttempts(request);
  let patch = {};
  let resolvedAttempt = 0;
  for (let index = 0; index < attempts.length; index += 1) {
    const raw = await rpc('bulk_timesheet_row_patch_v1', { p_filters: attempts[index] }, { timeoutMs: boundedTimeout(1500) });
    patch = unwrapBulkRowPatch(raw);
    if (trim(patch.row_key || patch.new_row_key)) {
      resolvedAttempt = index + 1;
      break;
    }
  }

  if (!trim(patch.row_key || patch.new_row_key)) {
    return {
      ok: true,
      outcome: 'DELETED',
      changed: true,
      eligible_for_surface: false,
      reason: 'ROW_NOT_FOUND',
      previous_row_key: request.row_key || request.previous_row_key || null,
      row_key: null,
      signature: null,
      target_section: null,
      row: null,
      resolution_attempts: attempts.length
    };
  }

  const identity = canonicalIdentity(patch);
  const fn = request.surface === 'bulk_process' ? 'bulk_process_dataset_v1' : 'bulk_authorise_dataset_v1';
  const datasetRaw = await rpc(fn, { p_filters: datasetFilters(request, identity) }, { timeoutMs: boundedTimeout(3000) });
  const membership = membershipFromDataset(request.surface, datasetRaw, identity);
  const signature = trim(
    membership.row?.backend_row_signature || membership.row?.row_signature ||
    patch.backend_row_signature || patch.row_signature || patch.mutation_row_signature
  );
  const oldKey = request.row_key || request.previous_row_key || null;

  if (!membership.row) {
    return {
      ok: true,
      outcome: 'REMOVED',
      changed: true,
      eligible_for_surface: false,
      reason: removalReason(request.surface, patch),
      previous_row_key: oldKey,
      row_key: identity.rowKey || null,
      signature: signature || null,
      target_section: null,
      row: { ...patch },
      resolution_attempts: resolvedAttempt
    };
  }

  const allowedSections = request.surface === 'bulk_process' ? PROCESS_SECTIONS : AUTHORISE_VISIBLE_SECTIONS;
  const currentSection = allowedSections.has(trim(request.current_section).toLowerCase())
    ? trim(request.current_section).toLowerCase()
    : null;
  const moved = !!(currentSection && currentSection !== membership.section);
  const changed = moved || !request.known_signature || request.known_signature !== signature || (!!oldKey && oldKey !== identity.rowKey);

  return {
    ok: true,
    outcome: moved ? 'MOVED' : 'CURRENT',
    changed,
    eligible_for_surface: true,
    reason: moved ? upper(membership.section.replace('_eligible', '')) : null,
    previous_row_key: oldKey,
    row_key: identity.rowKey || null,
    signature: signature || null,
    target_section: membership.section,
    row: { ...patch, ...membership.row },
    resolution_attempts: resolvedAttempt
  };
}

function jsonResponse(payload, status) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }
  });
}

export async function handleBulkRowFreshnessRequest(dependencies, env, req) {
  const deps = plainObject(dependencies);
  const user = await deps.requireUser(env, req, ['admin']);
  if (!user) return deps.withCORS(env, req, deps.unauthorized());

  const startedAt = Date.now();
  let request;
  try {
    request = buildBulkRowFreshnessRequest(req.url, user.id);
  } catch (error) {
    return deps.withCORS(env, req, jsonResponse({
      ok: false,
      error_code: error?.code || 'INVALID_REQUEST',
      message: trim(error?.message) || 'Invalid freshness request.'
    }, 400));
  }

  try {
    const payload = await resolveBulkRowFreshness(
      request,
      (fn, args, options) => deps.sbRpc(env, fn, args, options)
    );
    console.info('[TS][BULK-ROW-FRESHNESS]', {
      surface: request.surface,
      outcome: payload.outcome,
      changed: payload.changed,
      eligible: payload.eligible_for_surface,
      resolution_attempts: payload.resolution_attempts,
      elapsed_ms: Math.max(0, Date.now() - startedAt)
    });
    return deps.withCORS(env, req, jsonResponse(payload, 200));
  } catch (error) {
    const timedOut = Number(error?.status || 0) === 408 || /timed out|timeout/i.test(trim(error?.message));
    console.warn('[TS][BULK-ROW-FRESHNESS][FAILED]', {
      surface: request.surface,
      timeout: timedOut,
      elapsed_ms: Math.max(0, Date.now() - startedAt)
    });
    return deps.withCORS(env, req, jsonResponse({
      ok: false,
      soft_failure: true,
      error_code: timedOut ? 'FRESHNESS_TIMEOUT' : 'FRESHNESS_FAILED',
      message: 'Current server state could not be confirmed. Please click the row to retry.'
    }, timedOut ? 504 : 502));
  }
}
