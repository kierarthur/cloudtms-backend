import {
  buildInvoiceDocumentDownloadUrl,
  createInvoiceDocumentAccessToken,
  verifyInvoiceDocumentAccessToken
} from './invoice-document-access.js';
import {
  isInvoiceAsyncPipelineEnabled,
  nudgeInvoiceOperations
} from './invoice-queue-runtime.js';
import {
  isInvoiceAsyncUserAllowed,
  parseInvoiceAsyncAllowedUserIds
} from './invoice-queue-security.js';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const INVOICE_ASYNC_CONTRACT_VERSION = 'INVOICE_ASYNC_BACKEND_V7';
const JSON_HEADERS = Object.freeze({
  'content-type': 'application/json; charset=utf-8',
  'x-invoice-async-contract-version': INVOICE_ASYNC_CONTRACT_VERSION
});

function jsonResponse(body, status = 200, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...JSON_HEADERS, ...headers }
  });
}

function rpcValue(value) {
  if (Array.isArray(value)) {
    if (value.length === 1 && value[0] && typeof value[0] === 'object') {
      const values = Object.values(value[0]);
      if (values.length === 1 && (Array.isArray(values[0]) || typeof values[0] === 'object')) {
        return values[0];
      }
    }
    return value;
  }
  if (value && Array.isArray(value.rows)) return rpcValue(value.rows);
  if (value && Array.isArray(value.data)) return rpcValue(value.data);
  return value;
}

function canonicalUuidArray(values) {
  const input = Array.isArray(values) ? values : [];
  const ids = input.map(value => String(value || '').trim().toLowerCase());
  if (!ids.length || ids.some(value => !UUID_PATTERN.test(value))) {
    throw new Error('VALID_UUID_ARRAY_REQUIRED');
  }
  return [...new Set(ids)].sort();
}

function boolValue(value, fallback = false) {
  if (value === true || value === false) return value;
  const normalised = String(value ?? '').trim().toLowerCase();
  if (!normalised) return fallback;
  return ['1', 'true', 'yes', 'y', 'on'].includes(normalised);
}

function canonicalEmailArray(value) {
  const values = Array.isArray(value)
    ? value
    : String(value || '').split(/[;,]/g);
  const emails = values
    .map(item => String(item || '').trim().toLowerCase())
    .filter(Boolean);
  if (emails.some(email => !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))) {
    throw new Error('INVALID_RECIPIENT_EMAIL');
  }
  return [...new Set(emails)].sort();
}

function canonicalDeliveryPolicy(value) {
  const policy = String(value || 'ATTACH').trim().toUpperCase();
  if (!['ATTACH','SPLIT','SECURE_LINK'].includes(policy)) throw new Error('DELIVERY_POLICY_INVALID');
  return policy;
}

function commandToken(req, body = {}) {
  return String(
    body.command_token
    || body.delivery_request_token
    || req.headers.get('idempotency-key')
    || req.headers.get('x-idempotency-key')
    || crypto.randomUUID()
  ).slice(0, 200);
}

function generationCommandFromBody(req, body, commandType = 'GENERATE_SELECTED') {
  const canonical = body.canonical_command && typeof body.canonical_command === 'object'
    ? body.canonical_command
    : {};
  const sourceIds = body.source_ids || body.timesheet_ids
    || canonical.source_ids || canonical.canonical_source_ids
    || canonical.timesheet_ids;
  const command = {
    command_type: commandType,
    source_ids: canonicalUuidArray(sourceIds),
    consolidation_mode:
      body.consolidation_mode || canonical.consolidation_mode || 'NONE',
    allow_early: boolValue(body.allow_early ?? canonical.allow_early, false),
    target_invoice_week:
      body.target_invoice_week || body.invoice_week_start || canonical.target_invoice_week || undefined,
    command_token: commandToken(req, body)
  };
  const optional = {
    scope_key: body.scope_key || canonical.scope_key || canonical.group_key,
    group_key: body.group_key || canonical.group_key || canonical.scope_key,
    canonical_source_members: body.canonical_source_members || canonical.canonical_source_members,
    client_id: body.client_id || canonical.client_id,
    contract_id: body.contract_id || canonical.contract_id,
    contract_ids: body.contract_ids || canonical.contract_ids,
    natural_source_week: body.natural_source_week || canonical.natural_source_week,
    invoice_stream: body.invoice_stream || canonical.invoice_stream || canonical.stream,
    source_revision: body.source_revision || canonical.source_revision || canonical.canonical_source_revision,
    source_revision_hash: body.source_revision_hash || canonical.source_revision_hash || canonical.canonical_source_revision
  };
  for (const [key, value] of Object.entries(optional)) {
    if (value !== undefined && value !== null && value !== '') command[key] = structuredClone(value);
  }
  return command;
}

async function sha256Text(value) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(String(value)));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

const INVOICE_BATCH_FILTER_KEYS = Object.freeze([
  'client_ids', 'candidate_ids', 'week_endings', 'week_ending_from', 'week_ending_to',
  'status_codes', 'blocker_codes', 'search', 'allow_early', 'display_mode'
]);
const INVOICE_BATCH_SORT_KEYS = Object.freeze(['group_preset', 'sort_key', 'sort_direction']);
const INVOICE_BATCH_QUERY_CONTROL_KEYS = Object.freeze([
  'contract_version', 'action', 'mode', 'snapshot_at_utc', 'page_size', 'limit',
  'cursor', 'page_cursor', 'pageCursor', 'facets', 'filters', 'sort', 'selection',
  'after_selection_key', 'filter_hash'
]);
const INVOICE_BATCH_GROUP_PRESETS = Object.freeze([
  'WEEK_CLIENT_CANDIDATE', 'CLIENT_WEEK_CANDIDATE', 'CANDIDATE_WEEK_CLIENT', 'STATUS_WEEK_CLIENT'
]);
const INVOICE_BATCH_GENERATE_SORT_KEYS = Object.freeze([
  'WEEK_ENDING_DATE', 'CLIENT_NAME', 'CANDIDATE_NAME', 'TOTAL_EX_VAT', 'TOTAL_INC_VAT', 'STATUS'
]);
const INVOICE_BATCH_ISSUE_SORT_KEYS = Object.freeze([...INVOICE_BATCH_GENERATE_SORT_KEYS, 'INVOICE_NUMBER']);
const INVOICE_BATCH_GENERATE_STATUS_CODES = Object.freeze(['READY', 'BLOCKED', 'IN_PROGRESS', 'STALE', 'FAILED']);
const INVOICE_BATCH_ISSUE_STATUS_CODES = Object.freeze(['READY', 'BLOCKED', 'IN_PROGRESS', 'STALE', 'FAILED']);
const INVOICE_OPERATION_RESULT_CATEGORIES = Object.freeze([
  'ALL', 'READY', 'IN_PROGRESS', 'COMPLETED', 'BLOCKED', 'FAILED', 'CHANGED',
  'ISSUED', 'ISSUED_SEND_BLOCKED'
]);

function invoiceBatchContractError(code) {
  return Object.assign(new Error(code), { code });
}

function invoiceBatchSourceValues(source, key) {
  if (source instanceof URLSearchParams) {
    return [key, `${key}[]`].flatMap(name => source.getAll(name))
      .flatMap(value => String(value || '').split(','));
  }
  const nested = source?.filters && typeof source.filters === 'object' && !Array.isArray(source.filters)
    ? source.filters
    : null;
  const value = nested && Object.hasOwn(nested, key) ? nested[key] : source?.[key];
  if (value === undefined || value === null || value === '') return [];
  return (Array.isArray(value) ? value : String(value).split(','))
    .flatMap(item => typeof item === 'string' ? item.split(',') : [item]);
}

function invoiceBatchSourceScalar(source, key) {
  const values = invoiceBatchSourceValues(source, key);
  return values.length ? values[values.length - 1] : undefined;
}

function validateInvoiceBatchSourceFields(source) {
  const allowed = new Set([
    ...INVOICE_BATCH_FILTER_KEYS,
    ...INVOICE_BATCH_FILTER_KEYS.map(key => `${key}[]`),
    ...INVOICE_BATCH_SORT_KEYS,
    ...INVOICE_BATCH_QUERY_CONTROL_KEYS
  ]);
  const keys = source instanceof URLSearchParams ? [...new Set(source.keys())] : Object.keys(source || {});
  if (keys.some(key => !allowed.has(key))) throw invoiceBatchContractError('BATCH_FILTER_FIELD_UNSUPPORTED');
  if (!(source instanceof URLSearchParams)) {
    for (const [container, allowedKeys, errorCode] of [
      [source?.filters, INVOICE_BATCH_FILTER_KEYS, 'BATCH_FILTER_FIELD_UNSUPPORTED'],
      [source?.sort, INVOICE_BATCH_SORT_KEYS, 'INVOICE_BATCH_SORT_FIELD_UNSUPPORTED']
    ]) {
      if (container === undefined) continue;
      if (!container || typeof container !== 'object' || Array.isArray(container)) {
        throw invoiceBatchContractError(errorCode);
      }
      if (Object.keys(container).some(key => !allowedKeys.includes(key))) {
        throw invoiceBatchContractError(errorCode);
      }
    }
  }
}

function normaliseInvoiceBatchBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (value === true || value === false) return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  throw invoiceBatchContractError('BATCH_FILTER_BOOLEAN_INVALID');
}

function normaliseInvoiceBatchDate(value, errorCode) {
  const text = String(value || '').trim();
  if (!text) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) throw invoiceBatchContractError(errorCode);
  const date = new Date(`${text}T00:00:00.000Z`);
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== text) {
    throw invoiceBatchContractError(errorCode);
  }
  return text;
}

function normaliseInvoiceBatchSnapshotUtc(value, fallback = null) {
  const timestamp = new Date(value || fallback || '');
  if (!Number.isFinite(timestamp.getTime())) throw invoiceBatchContractError('BATCH_QUERY_SNAPSHOT_INVALID');
  return timestamp.toISOString();
}

function normaliseInvoiceBatchArray(values, options = {}) {
  const {
    maximum = 500,
    normalise = value => String(value || '').trim(),
    validate = value => !!value,
    errorCode = 'BATCH_FILTER_VALUE_INVALID'
  } = options;
  if (values.length > maximum) throw invoiceBatchContractError('BATCH_FILTER_ARRAY_LIMIT_EXCEEDED');
  const normalised = values.map(normalise);
  if (normalised.some(value => !validate(value))) throw invoiceBatchContractError(errorCode);
  return [...new Set(normalised)].sort();
}

function normaliseInvoiceBatchFilters(source, action = 'GENERATE') {
  if (!source || (typeof source !== 'object' && !(source instanceof URLSearchParams))) {
    throw invoiceBatchContractError('BATCH_QUERY_INVALID');
  }
  validateInvoiceBatchSourceFields(source);
  const normalizedAction = String(action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(normalizedAction)) throw invoiceBatchContractError('BATCH_QUERY_ACTION_MISMATCH');
  const uuidOptions = {
    normalise: value => String(value || '').trim().toLowerCase(),
    validate: value => UUID_PATTERN.test(value),
    errorCode: 'BATCH_FILTER_UUID_INVALID'
  };
  const weekOptions = {
    normalise: value => normaliseInvoiceBatchDate(value, 'BATCH_FILTER_DATE_INVALID'),
    validate: value => !!value,
    errorCode: 'BATCH_FILTER_DATE_INVALID'
  };
  const codeOptions = {
    maximum: 100,
    normalise: value => String(value || '').trim().toUpperCase(),
    validate: value => /^[A-Z0-9][A-Z0-9_:-]{0,119}$/.test(value),
    errorCode: 'BATCH_FILTER_CODE_INVALID'
  };
  const statusCodes = normaliseInvoiceBatchArray(invoiceBatchSourceValues(source, 'status_codes'), codeOptions);
  const allowedStatuses = normalizedAction === 'ISSUE'
    ? INVOICE_BATCH_ISSUE_STATUS_CODES
    : INVOICE_BATCH_GENERATE_STATUS_CODES;
  if (statusCodes.some(code => !allowedStatuses.includes(code))) {
    throw invoiceBatchContractError('BATCH_FILTER_STATUS_UNSUPPORTED');
  }
  const weekEndingFrom = normaliseInvoiceBatchDate(invoiceBatchSourceScalar(source, 'week_ending_from'), 'BATCH_FILTER_DATE_INVALID');
  const weekEndingTo = normaliseInvoiceBatchDate(invoiceBatchSourceScalar(source, 'week_ending_to'), 'BATCH_FILTER_DATE_INVALID');
  if (weekEndingFrom && weekEndingTo && weekEndingFrom > weekEndingTo) {
    throw invoiceBatchContractError('BATCH_FILTER_DATE_RANGE_INVALID');
  }
  const search = String(invoiceBatchSourceScalar(source, 'search') || '').trim();
  if (search.length > 200) throw invoiceBatchContractError('BATCH_FILTER_SEARCH_TOO_LONG');
  const displayMode = String(invoiceBatchSourceScalar(source, 'display_mode') || 'ALL').trim().toUpperCase();
  if (!['ALL', 'READY', 'BLOCKED'].includes(displayMode)) {
    throw invoiceBatchContractError('INVOICE_BATCH_DISPLAY_MODE_INVALID');
  }
  return {
    client_ids: normaliseInvoiceBatchArray(invoiceBatchSourceValues(source, 'client_ids'), uuidOptions),
    candidate_ids: normaliseInvoiceBatchArray(invoiceBatchSourceValues(source, 'candidate_ids'), uuidOptions),
    week_endings: normaliseInvoiceBatchArray(invoiceBatchSourceValues(source, 'week_endings'), weekOptions),
    week_ending_from: weekEndingFrom,
    week_ending_to: weekEndingTo,
    status_codes: statusCodes,
    blocker_codes: normaliseInvoiceBatchArray(invoiceBatchSourceValues(source, 'blocker_codes'), codeOptions),
    search: search || null,
    allow_early: normaliseInvoiceBatchBoolean(invoiceBatchSourceScalar(source, 'allow_early'), false),
    display_mode: displayMode
  };
}

function normaliseInvoiceBatchSort(source, action = 'GENERATE') {
  if (!source || (typeof source !== 'object' && !(source instanceof URLSearchParams))) {
    throw invoiceBatchContractError('BATCH_QUERY_INVALID');
  }
  validateInvoiceBatchSourceFields(source);
  const normalizedAction = String(action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(normalizedAction)) throw invoiceBatchContractError('BATCH_QUERY_ACTION_MISMATCH');
  const nested = source?.sort && typeof source.sort === 'object' && !Array.isArray(source.sort) ? source.sort : null;
  const scalar = key => nested && Object.hasOwn(nested, key) ? nested[key] : invoiceBatchSourceScalar(source, key);
  const groupPreset = String(scalar('group_preset') || 'WEEK_CLIENT_CANDIDATE').trim().toUpperCase();
  const sortKey = String(scalar('sort_key') || 'WEEK_ENDING_DATE').trim().toUpperCase();
  const sortDirection = String(scalar('sort_direction') || 'ASC').trim().toUpperCase();
  if (!INVOICE_BATCH_GROUP_PRESETS.includes(groupPreset)) throw invoiceBatchContractError('INVOICE_BATCH_GROUP_PRESET_INVALID');
  const allowedSortKeys = normalizedAction === 'ISSUE' ? INVOICE_BATCH_ISSUE_SORT_KEYS : INVOICE_BATCH_GENERATE_SORT_KEYS;
  if (!allowedSortKeys.includes(sortKey)) throw invoiceBatchContractError('INVOICE_BATCH_SORT_KEY_INVALID');
  if (!['ASC', 'DESC'].includes(sortDirection)) throw invoiceBatchContractError('INVOICE_BATCH_SORT_DIRECTION_INVALID');
  return { group_preset: groupPreset, sort_key: sortKey, sort_direction: sortDirection };
}
function normaliseInvoiceBatchSelectionRules(source) {
  const selection = source?.selection ?? source;
  if (!selection || typeof selection !== 'object' || Array.isArray(selection)) {
    throw invoiceBatchContractError('BATCH_SELECTION_INVALID');
  }
  if (Object.keys(selection).some(key => !['contract_version', 'mode', 'default_selected', 'rules'].includes(key))) {
    throw invoiceBatchContractError('BATCH_SELECTION_UNKNOWN_FIELD');
  }
  if (selection.contract_version !== 'INVOICE_BATCH_SELECTION_V1') {
    throw invoiceBatchContractError('BATCH_SELECTION_CONTRACT_INVALID');
  }
  if (String(selection.mode || '').trim().toUpperCase() !== 'IMPLICIT_ALL') {
    throw invoiceBatchContractError('BATCH_SELECTION_MODE_UNSUPPORTED');
  }
  if (selection.default_selected !== true) throw invoiceBatchContractError('BATCH_SELECTION_DEFAULT_INVALID');
  if (!Array.isArray(selection.rules)) throw invoiceBatchContractError('BATCH_SELECTION_RULES_INVALID');
  if (selection.rules.length > 10000) throw invoiceBatchContractError('BATCH_SELECTION_RULE_LIMIT_EXCEEDED');
  const requiredFieldsByType = {
    ROW: ['selection_key'],
    WEEK: ['week_ending_date'],
    CLIENT: ['client_id'],
    CANDIDATE: ['candidate_id'],
    WEEK_CLIENT: ['week_ending_date', 'client_id'],
    WEEK_CLIENT_CANDIDATE: ['week_ending_date', 'client_id', 'candidate_id']
  };
  let previousSequence = 0;
  const seenSequences = new Set();
  const rules = selection.rules.map(rule => {
    if (!rule || typeof rule !== 'object' || Array.isArray(rule)) {
      throw invoiceBatchContractError('BATCH_SELECTION_RULE_INVALID');
    }
    if (Object.keys(rule).some(key => !['sequence', 'action', 'selector'].includes(key))) {
      throw invoiceBatchContractError('BATCH_SELECTION_RULE_UNKNOWN_FIELD');
    }
    const sequence = Number(rule.sequence);
    if (!Number.isSafeInteger(sequence) || sequence < 1 || sequence > 999999999) {
      throw invoiceBatchContractError('BATCH_SELECTION_RULE_INVALID');
    }
    if (seenSequences.has(sequence)) throw invoiceBatchContractError('BATCH_SELECTION_RULE_SEQUENCE_DUPLICATE');
    if (sequence <= previousSequence) throw invoiceBatchContractError('BATCH_SELECTION_RULE_SEQUENCE_INVALID');
    previousSequence = sequence;
    seenSequences.add(sequence);
    const action = String(rule.action || '').trim().toUpperCase();
    if (!['INCLUDE', 'EXCLUDE'].includes(action)) throw invoiceBatchContractError('BATCH_SELECTION_RULE_INVALID');
    const selector = rule.selector;
    if (!selector || typeof selector !== 'object' || Array.isArray(selector)) {
      throw invoiceBatchContractError('BATCH_SELECTION_RULE_INVALID');
    }
    if (Object.keys(selector).some(key => !['type', 'selection_key', 'week_ending_date', 'client_id', 'candidate_id'].includes(key))) {
      throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_UNKNOWN_FIELD');
    }
    const type = String(selector.type || '').trim().toUpperCase();
    const requiredFields = requiredFieldsByType[type];
    if (!requiredFields) throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
    const suppliedFields = Object.keys(selector).filter(key => key !== 'type' && selector[key] !== null && selector[key] !== '');
    if (requiredFields.some(key => !suppliedFields.includes(key)) || suppliedFields.some(key => !requiredFields.includes(key))) {
      throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
    }
    const normalizedSelector = { type };
    for (const key of requiredFields) {
      if (key === 'selection_key') {
        const value = String(selector[key] || '').trim();
        if (!value || value.length > 512) throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
        normalizedSelector[key] = value;
      } else if (key === 'week_ending_date') {
        normalizedSelector[key] = normaliseInvoiceBatchDate(selector[key], 'BATCH_SELECTION_SELECTOR_INVALID');
      } else {
        const value = String(selector[key] || '').trim().toLowerCase();
        if (!UUID_PATTERN.test(value)) throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
        normalizedSelector[key] = value;
      }
    }
    return { sequence, action, selector: normalizedSelector };
  });
  return {
    contract_version: 'INVOICE_BATCH_SELECTION_V1',
    mode: 'IMPLICIT_ALL',
    default_selected: true,
    rules
  };
}

function postgresJsonbTextForInvoiceBatch(value) {
  if (Array.isArray(value)) return `[${value.map(postgresJsonbTextForInvoiceBatch).join(', ')}]`;
  if (value && typeof value === 'object') {
    const encoder = new TextEncoder();
    const compareKeys = (left, right) => {
      const a = encoder.encode(left);
      const b = encoder.encode(right);
      if (a.byteLength !== b.byteLength) return a.byteLength - b.byteLength;
      for (let index = 0; index < a.byteLength; index += 1) {
        if (a[index] !== b[index]) return a[index] - b[index];
      }
      return 0;
    };
    return `{${Object.keys(value).sort(compareKeys).map(key => `${JSON.stringify(key)}: ${postgresJsonbTextForInvoiceBatch(value[key])}`).join(', ')}}`;
  }
  return JSON.stringify(value);
}

async function hashInvoiceBatchQuery(action, filters, sort, snapshotAtUtc, options = {}) {
  const normalizedAction = String(action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(normalizedAction)) throw invoiceBatchContractError('BATCH_QUERY_ACTION_MISMATCH');
  if (options.db_candidate_filter_hash === true) {
    return sha256Text(`${postgresJsonbTextForInvoiceBatch(filters)}|${postgresJsonbTextForInvoiceBatch(sort)}|${normalizedAction}`);
  }
  const snapshot = new Date(snapshotAtUtc);
  if (!Number.isFinite(snapshot.getTime())) throw invoiceBatchContractError('BATCH_QUERY_SNAPSHOT_INVALID');
  return sha256Text(postgresJsonbTextForInvoiceBatch({
    action: normalizedAction,
    filters,
    sort,
    snapshot_at_utc: snapshot.toISOString()
  }));
}
function normaliseInvoiceBatchCursorValues(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const allowed = ['after_selection_key', 'after_sort_date', 'after_sort_text', 'after_sort_numeric', 'after_chunk_id'];
  if (Object.keys(value).some(key => !allowed.includes(key))) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const result = {};
  if (value.after_selection_key !== undefined && value.after_selection_key !== null) {
    const text = String(value.after_selection_key).trim();
    if (!text || text.length > 512) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_selection_key = text;
  }
  if (value.after_sort_date !== undefined && value.after_sort_date !== null) {
    result.after_sort_date = normaliseInvoiceBatchDate(value.after_sort_date, 'BATCH_CURSOR_INVALID');
  }
  if (value.after_sort_text !== undefined && value.after_sort_text !== null) {
    const text = String(value.after_sort_text);
    if (text.length > 512) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_sort_text = text;
  }
  if (value.after_sort_numeric !== undefined && value.after_sort_numeric !== null) {
    const text = String(value.after_sort_numeric).trim();
    if (!/^[+-]?\d+(?:\.\d+)?$/.test(text) || text.length > 100) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_sort_numeric = text;
  }
  if (value.after_chunk_id !== undefined && value.after_chunk_id !== null) {
    const text = String(value.after_chunk_id).trim().toLowerCase();
    if (!UUID_PATTERN.test(text)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_chunk_id = text;
  }
  if (!Object.keys(result).length) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  return result;
}

async function signInvoiceBatchCursor(secret, encodedPayload) {
  if (!secret || String(secret).length < 32) throw invoiceBatchContractError('BATCH_CURSOR_SECRET_INVALID');
  const key = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(String(secret)),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign(
    'HMAC', key, new TextEncoder().encode(`invoice-batch-cursor-v1.${encodedPayload}`)
  );
  return encodeOutboxCursorPart(new Uint8Array(signature));
}

async function encodeInvoiceBatchCursor(env, input) {
  const action = String(input?.action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(action)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const snapshot = new Date(input?.snapshot_at_utc);
  if (!Number.isFinite(snapshot.getTime())) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const filterHash = String(input?.filter_hash || '').trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(filterHash)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const sort = normaliseInvoiceBatchSort({ sort: input?.sort || {} }, action);
  const cursor = normaliseInvoiceBatchCursorValues(input?.next_cursor_values || input?.cursor || {});
  const resultCategory = input?.result_category ? String(input.result_category).trim().toUpperCase().slice(0, 80) : null;
  const payload = {
    version: 1,
    action,
    snapshot_at_utc: snapshot.toISOString(),
    filter_hash: filterHash,
    sort,
    cursor,
    ...(resultCategory ? { result_category: resultCategory } : {})
  };
  const encodedPayload = encodeOutboxCursorPart(new TextEncoder().encode(JSON.stringify(payload)));
  const signature = await signInvoiceBatchCursor(env?.SESSION_TOKEN_SECRET, encodedPayload);
  return `${encodedPayload}.${signature}`;
}

async function decodeInvoiceBatchCursor(env, token, expected = {}) {
  const tokenText = String(token || '');
  if (tokenText.length > 8192) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const parts = tokenText.split('.');
  if (parts.length !== 2 || parts.some(part => !part)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  let expectedSignature;
  let actualSignature;
  try {
    expectedSignature = decodeOutboxCursorPart(await signInvoiceBatchCursor(env?.SESSION_TOKEN_SECRET, parts[0]));
    actualSignature = decodeOutboxCursorPart(parts[1]);
  } catch (error) {
    if (error?.code === 'BATCH_CURSOR_SECRET_INVALID') throw error;
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  if (expectedSignature.byteLength !== actualSignature.byteLength) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  let difference = 0;
  for (let index = 0; index < expectedSignature.byteLength; index += 1) difference |= expectedSignature[index] ^ actualSignature[index];
  if (difference !== 0) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  let payload;
  try {
    payload = JSON.parse(new TextDecoder().decode(decodeOutboxCursorPart(parts[0])));
  } catch {
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  if (payload?.version !== 1) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const action = String(payload.action || '').trim().toUpperCase();
  const expectedAction = String(expected.action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(action) || (expectedAction && action !== expectedAction)) {
    throw invoiceBatchContractError('BATCH_CURSOR_ACTION_MISMATCH');
  }
  const snapshot = new Date(payload.snapshot_at_utc);
  if (!Number.isFinite(snapshot.getTime())) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const filterHash = String(payload.filter_hash || '').trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(filterHash)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  if (expected.filter_hash && filterHash !== String(expected.filter_hash).trim().toLowerCase()) {
    throw invoiceBatchContractError('BATCH_CURSOR_FILTER_MISMATCH');
  }
  const sort = normaliseInvoiceBatchSort({ sort: payload.sort || {} }, action);
  if (expected.sort) {
    const expectedSort = normaliseInvoiceBatchSort({ sort: expected.sort }, action);
    if (postgresJsonbTextForInvoiceBatch(sort) !== postgresJsonbTextForInvoiceBatch(expectedSort)) {
      throw invoiceBatchContractError('BATCH_CURSOR_SORT_MISMATCH');
    }
  }
  const resultCategory = payload.result_category ? String(payload.result_category).trim().toUpperCase() : null;
  if (expected.result_category && resultCategory !== String(expected.result_category).trim().toUpperCase()) {
    throw invoiceBatchContractError('BATCH_CURSOR_FILTER_MISMATCH');
  }
  const cursor = normaliseInvoiceBatchCursorValues(payload.cursor || {});
  return {
    version: 1,
    action,
    snapshot_at_utc: snapshot.toISOString(),
    filter_hash: filterHash,
    sort,
    cursor,
    next_cursor_values: cursor,
    ...(resultCategory ? { result_category: resultCategory } : {})
  };
}
function candidateGroupsFromRpc(value) {
  const isV1Envelope = candidate => !!(
    candidate
    && typeof candidate === 'object'
    && !Array.isArray(candidate)
    && candidate.contract_version === 'INVOICE_BATCH_CANDIDATES_V1'
    && Array.isArray(candidate.rows)
  );

  const unwrapV1Envelope = candidate => {
    if (isV1Envelope(candidate)) return candidate;

    if (Array.isArray(candidate)) {
      if (candidate.length !== 1) return null;
      if (isV1Envelope(candidate[0])) return candidate[0];
      if (candidate[0] && typeof candidate[0] === 'object' && !Array.isArray(candidate[0])) {
        const innerValues = Object.values(candidate[0]);
        if (innerValues.length === 1) return unwrapV1Envelope(innerValues[0]);
      }
      return null;
    }

    if (candidate && typeof candidate === 'object') {
      if (Array.isArray(candidate.rows)) {
        const rowEnvelope = unwrapV1Envelope(candidate.rows);
        if (rowEnvelope) return rowEnvelope;
      }
      if (Array.isArray(candidate.data)) {
        const dataEnvelope = unwrapV1Envelope(candidate.data);
        if (dataEnvelope) return dataEnvelope;
      }
      const innerValues = Object.values(candidate);
      if (innerValues.length === 1) return unwrapV1Envelope(innerValues[0]);
    }

    return null;
  };

  const rawEnvelope = unwrapV1Envelope(value) || unwrapV1Envelope(rpcValue(value));

  if (rawEnvelope) {
    return {
      kind: 'V1',
      legacy: false,
      contract_version: rawEnvelope.contract_version,
      action: String(rawEnvelope.action || '').toUpperCase(),
      mode: rawEnvelope.mode || null,
      snapshot_at_utc: rawEnvelope.snapshot_at_utc || null,
      normalised_filter: rawEnvelope.normalised_filter || rawEnvelope.normalized_filter || {},
      normalized_filter: rawEnvelope.normalised_filter || rawEnvelope.normalized_filter || {},
      normalised_sort: rawEnvelope.normalised_sort || rawEnvelope.normalized_sort || {},
      normalized_sort: rawEnvelope.normalised_sort || rawEnvelope.normalized_sort || {},
      filter_hash: rawEnvelope.filter_hash || null,
      rows: rawEnvelope.rows,
      page: rawEnvelope.page || {},
      totals: rawEnvelope.totals || {},
      facets: rawEnvelope.facets || {},
      selection_seed: rawEnvelope.selection_seed || {
        mode: 'IMPLICIT_ALL',
        default_selected: true
      },
      raw: rawEnvelope
    };
  }

  const resolved = rpcValue(value);
  const clients = Array.isArray(resolved)
    ? resolved
    : (Array.isArray(resolved?.clients) ? resolved.clients : []);

  const groups = clients.flatMap(client => {
    if (Array.isArray(client?.groups)) {
      return client.groups.map(group => ({
        ...group,
        client_id: group.client_id || client.client_id,
        client_name: group.client_name || client.client_name
      }));
    }
    if (Array.isArray(client?.weeks)) {
      return client.weeks.flatMap(week => {
        if (Array.isArray(week?.groups)) {
          return week.groups.map(group => ({
            ...group,
            client_id: group.client_id || client.client_id,
            client_name: group.client_name || client.client_name,
            week_ending_date: group.week_ending_date || week.week_ending_date,
            invoice_week_start: group.invoice_week_start || week.invoice_week_start
          }));
        }
        if (Array.isArray(week?.invoices)) {
          return week.invoices.map(invoice => ({
            ...invoice,
            client_id: invoice.client_id || client.client_id,
            client_name: invoice.client_name || client.client_name,
            week_ending_date: invoice.week_ending_date || week.week_ending_date,
            invoice_week_start: invoice.invoice_week_start || week.invoice_week_start
          }));
        }
        return [];
      });
    }
    return client?.group_key || client?.scope_key || client?.invoice_id ? [client] : [];
  });

  Object.defineProperties(groups, {
    kind: { value: 'LEGACY', enumerable: false },
    legacy: { value: true, enumerable: false },
    clients: { value: clients, enumerable: false },
    groups: { value: groups, enumerable: false },
    rows: { value: groups, enumerable: false },
    raw: { value: resolved, enumerable: false }
  });
  return groups;
}

async function startCommands(env, req, ctx, user, commands, deps, lanes = ['ALL'], options = {}) {
  const raw = await deps.rpc('invoice_operation_start_batch', {
    p_commands: commands,
    p_actor_user_id: user.id,
    p_now_utc: new Date().toISOString()
  });
  const value = rpcValue(raw);
  const operations = Array.isArray(value) ? value : (value ? [value] : []);

  if (options.commandContextByNo instanceof Map) {
    const returned = operations.map(row => Number(row?.command_no));
    const expected = [...options.commandContextByNo.keys()];
    if (
      returned.some(value => !Number.isSafeInteger(value) || !options.commandContextByNo.has(value))
      || new Set(returned).size !== returned.length
      || expected.some(value => !returned.includes(value))
    ) {
      throw Object.assign(new Error('INVOICE_START_RESULT_CORRELATION_INVALID'), {
        code: 'INVOICE_START_RESULT_CORRELATION_INVALID'
      });
    }
  }

  const stableCode = row => String(
    row?.code
    || row?.error_code
    || row?.terminal_error?.code
    || row?.terminal_error
    || row?.error?.code
    || row?.error
    || ''
  ).trim().toUpperCase();
  const conflictCode = row => /(?:CONFLICT|SOURCE_CHANGED|STALE|ACTIVE_OPERATION|ALREADY_ACTIVE|NOT_READY|TERMINAL|BLOCKED|CURRENT_STATE_CHANGED)/.test(stableCode(row));
  const isSelectionRoot = row => row?.selection_contract_version === 'INVOICE_BATCH_SELECTION_V1'
    || row?.selection_expansion_pending === true
    || row?.input_json?.contract_version === 'INVOICE_BATCH_SELECTION_ROOT_V1';

  const accepted = operations.filter(row => row?.accepted === true);
  const created = accepted.filter(row => row?.created === true);
  const reusedActive = accepted.filter(row => row?.reused_active === true);
  const reusedReady = accepted.filter(row => row?.reused_ready === true);
  const blocked = operations.filter(row => row?.blocked === true || !!row?.terminal_error);
  const conflicted = operations.filter(row => row?.accepted !== true && !row?.blocked && !row?.terminal_error && conflictCode(row));
  const rejected = operations.filter(row => row?.accepted !== true && !row?.blocked && !row?.terminal_error && !conflictCode(row));
  const active = [...created, ...reusedActive]
    .filter(row => row?.operation_id)
    .filter((row, index, rows) => rows.findIndex(item => item.operation_id === row.operation_id) === index);
  const activeSelectionRoots = active.filter(isSelectionRoot);
  const activeNonSelection = active.filter(row => !isSelectionRoot(row));
  const additionalRejectedCount = Math.max(0, Number(options.additionalRejectedCount || 0));

  let nudge = { scheduled: false, code: 'NO_ACTIVE_WORK' };
  if (activeSelectionRoots.length || activeNonSelection.length) {
    const nudgeParts = [];
    if (activeSelectionRoots.length) {
      nudgeParts.push({
        scope: 'selection_roots',
        result: await nudgeInvoiceOperations(env, activeSelectionRoots, {
          ctx,
          rpc: deps.rpc,
          lanes: ['DATABASE'],
          priorityClass: options.priorityClass || 'INTERACTIVE'
        })
      });
    }
    if (activeNonSelection.length) {
      nudgeParts.push({
        scope: 'operations',
        result: await nudgeInvoiceOperations(env, activeNonSelection, {
          ctx,
          rpc: deps.rpc,
          lanes,
          priorityClass: options.priorityClass || 'INTERACTIVE'
        })
      });
    }
    nudge = nudgeParts.length === 1
      ? nudgeParts[0].result
      : { scheduled: nudgeParts.some(part => part.result?.scheduled), parts: nudgeParts };
  }

  let status = 202;
  if (!operations.length) status = 502;
  else if (!active.length && reusedReady.length && !blocked.length && !conflicted.length && !rejected.length) status = 200;
  else if (!active.length && rejected.length && !blocked.length && !conflicted.length && !reusedReady.length) status = 400;
  else if (!active.length && (blocked.length || conflicted.length) && !reusedReady.length) status = 409;
  else if ((active.length || reusedReady.length) && (blocked.length || conflicted.length || rejected.length || additionalRejectedCount)) status = 207;

  const operationIds = [...new Set(operations.map(row => row?.operation_id).filter(Boolean))];
  const selectionExpansionPending = operations.some(row => row?.selection_expansion_pending === true);
  const basePayload = {
    ok: active.length > 0 || reusedReady.length > 0,
    accepted: active.length > 0,
    accepted_count: active.length,
    created_count: created.length,
    reused_active_count: reusedActive.length,
    reused_ready_count: reusedReady.length,
    blocked_count: blocked.length,
    conflict_count: conflicted.length,
    rejected_count: rejected.length + conflicted.length + additionalRejectedCount,
    operation_ids: operationIds,
    root_operation_id: operationIds.length === 1 ? operationIds[0] : null,
    selection_expansion_pending: selectionExpansionPending,
    per_command_results: operations,
    nudge_state: nudge
  };
  const extraPayload = typeof options.extendResult === 'function'
    ? options.extendResult(basePayload, operations)
    : {};
  return jsonResponse({
    ...basePayload,
    ...(extraPayload && typeof extraPayload === 'object' ? extraPayload : {})
  }, status);
}

async function requireActor(env, req, deps, adminOnly = false) {
  return deps.requireUser(env, req, adminOnly ? ['admin'] : []);
}

async function parseBody(req) {
  try {
    const value = await req.json();
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
  } catch {
    return null;
  }
}

async function handleCandidates(envOrReq, reqOrDeps, depsOrRpcName, maybeRpcName) {
  const hasExplicitEnv = maybeRpcName !== undefined;
  const env = hasExplicitEnv ? envOrReq : (reqOrDeps?.env || depsOrRpcName?.env || null);
  const req = hasExplicitEnv ? reqOrDeps : envOrReq;
  const deps = hasExplicitEnv ? depsOrRpcName : reqOrDeps;
  const rpcName = hasExplicitEnv ? maybeRpcName : depsOrRpcName;
  const action = rpcName === 'invoice_batch_issue_candidates' ? 'ISSUE' : 'GENERATE';
  const url = new URL(req.url);
  const normaliseParams = new URLSearchParams(url.searchParams);
  // These are transport-only controls consumed by this HTTP handler. They are
  // not DB candidate filters/sorts and must not cause the strict normalisers
  // to reject an otherwise valid candidate-page request.
  normaliseParams.delete('facets');
  normaliseParams.delete('page_cursor');
  normaliseParams.delete('pageCursor');
  const filters = normaliseInvoiceBatchFilters(normaliseParams, action);
  const sort = normaliseInvoiceBatchSort(normaliseParams, action);
  const modeText = String(url.searchParams.get('mode') || '').trim().toUpperCase();
  if (modeText && !['PAGE', 'FACETS'].includes(modeText)) {
    throw invoiceBatchContractError('INVOICE_BATCH_QUERY_MODE_INVALID');
  }
  const facetText = String(url.searchParams.get('facets') || '').trim().toUpperCase();
  if (facetText && !['0', '1', 'FALSE', 'TRUE', 'NO', 'YES', 'OFF', 'ON'].includes(facetText)) {
    throw invoiceBatchContractError('BATCH_QUERY_INVALID');
  }
  const facetRequested = ['1', 'TRUE', 'YES', 'ON'].includes(facetText);
  const queryMode = facetRequested || modeText === 'FACETS' ? 'FACETS' : 'PAGE';
  const rawPageSize = Number(url.searchParams.get('page_size') || url.searchParams.get('limit') || 100);
  const pageSize = Number.isFinite(rawPageSize)
    ? Math.max(1, Math.min(100, Math.trunc(rawPageSize)))
    : 100;
  let snapshotAtUtc = normaliseInvoiceBatchSnapshotUtc(url.searchParams.get('snapshot_at_utc'), new Date().toISOString());
  let cursor = {};
  const expectedFilterHash = await hashInvoiceBatchQuery(action, filters, sort, snapshotAtUtc, {
    db_candidate_filter_hash: true
  });
  const cursorToken = url.searchParams.get('cursor') || url.searchParams.get('page_cursor') || url.searchParams.get('pageCursor');
  if (cursorToken) {
    if (!env) throw Object.assign(new Error('BATCH_CURSOR_ENV_REQUIRED'), { code: 'BATCH_CURSOR_ENV_REQUIRED' });
    const decoded = await decodeInvoiceBatchCursor(env, cursorToken, {
      action,
      filter_hash: expectedFilterHash,
      sort
    });
    cursor = decoded.cursor || decoded.next_cursor_values || {};
    snapshotAtUtc = decoded.snapshot_at_utc || snapshotAtUtc;
  }

  const pQuery = {
    contract_version: 'INVOICE_BATCH_QUERY_V1',
    action,
    mode: queryMode,
    snapshot_at_utc: snapshotAtUtc,
    allow_early: filters.allow_early,
    display_mode: filters.display_mode,
    page_size: pageSize,
    cursor,
    filters,
    sort
  };

  const result = await deps.rpc(rpcName, {
    p_allow_early: filters.allow_early,
    p_limit: pageSize,
    p_query: pQuery
  });
  const parsed = candidateGroupsFromRpc(result);
  if (parsed?.kind !== 'V1') {
    const candidates = Array.isArray(parsed) ? parsed : [];
    return jsonResponse({ ok: true, legacy: true, candidates, groups: candidates, limit: pageSize });
  }

  if (parsed.filter_hash && parsed.filter_hash !== expectedFilterHash) {
    throw Object.assign(new Error('BATCH_QUERY_FILTER_HASH_MISMATCH'), {
      code: 'BATCH_QUERY_FILTER_HASH_MISMATCH'
    });
  }

  const page = parsed.page || {};
  let signedNextCursor = null;
  if (page.has_more && page.next_cursor_values) {
    if (!env) throw Object.assign(new Error('BATCH_CURSOR_ENV_REQUIRED'), { code: 'BATCH_CURSOR_ENV_REQUIRED' });
    signedNextCursor = await encodeInvoiceBatchCursor(env, {
      action,
      snapshot_at_utc: parsed.snapshot_at_utc || snapshotAtUtc,
      filter_hash: parsed.filter_hash || expectedFilterHash,
      sort: parsed.normalised_sort || sort,
      next_cursor_values: page.next_cursor_values
    });
  }

  return jsonResponse({
    ok: true,
    contract_version: parsed.contract_version,
    action: parsed.action || action,
    mode: parsed.mode || queryMode,
    snapshot_at_utc: parsed.snapshot_at_utc || snapshotAtUtc,
    rows: parsed.rows,
    candidates: parsed.rows,
    page: {
      ...page,
      next_cursor: signedNextCursor
    },
    totals: parsed.totals,
    facets: parsed.facets,
    filter_hash: parsed.filter_hash || expectedFilterHash,
    normalised_filter: parsed.normalised_filter || filters,
    normalized_filter: parsed.normalised_filter || filters,
    normalised_sort: parsed.normalised_sort || sort,
    normalized_sort: parsed.normalised_sort || sort,
    selection_seed: parsed.selection_seed || { mode: 'IMPLICIT_ALL', default_selected: true }
  });
}

async function handleNhspCandidates(req, deps) {
  const url = new URL(req.url);
  const raw = await deps.rpc('invoice_batch_generate_candidates', {
    p_allow_early: boolValue(url.searchParams.get('allow_early'), false),
    p_limit: 5000
  });
  const clientId = String(url.searchParams.get('client_id') || '').trim();
  const candidates = candidateGroupsFromRpc(raw)
    .filter(row => {
      const command = row?.command_payload || {};
      const members = Array.isArray(command?.canonical_source_members)
        ? command.canonical_source_members
        : [];
      const nhsp = String(command?.command_type || '').toUpperCase() === 'GENERATE_NHSP'
        || String(command?.invoice_stream || command?.stream || '').toUpperCase().includes('NHSP')
        || members.some(member =>
          String(member?.source_type || member?.source_kind || '').toUpperCase().includes('NHSP'));
      return nhsp && (!clientId || String(row?.client_id || '') === clientId);
    })
    .map(row => ({
      client_id: row.client_id || null,
      group_key: row.group_key,
      canonical_source_members: row.canonical_source_members || row.command_payload?.canonical_source_members || [],
      canonical_source_revision: row.canonical_source_revision || row.command_payload?.source_revision || null,
      blocker_code: row.blocker_code || null,
      blocker_detail: row.blocker_detail || null,
      document_dependencies: Array.isArray(row.document_dependencies) ? row.document_dependencies : [],
      command_payload: row.command_payload,
      timesheets: Array.isArray(row.timesheets) ? row.timesheets : []
    }));
  return jsonResponse({
    ok: true,
    candidate_family: 'NHSP',
    candidates
  });
}

async function handleBatchGenerateConfirm(env, req, ctx, user, deps) {
  const body = await parseBody(req) || {};

  if (body.selection_contract && typeof body.selection_contract === 'object' && !Array.isArray(body.selection_contract)) {
    const rawContract = body.selection_contract;
    if (rawContract.contract_version !== 'INVOICE_BATCH_SELECTION_V1') {
      return jsonResponse({ error: 'BATCH_SELECTION_CONTRACT_INVALID' }, 400);
    }
    if (!rawContract.query || typeof rawContract.query !== 'object' || Array.isArray(rawContract.query)) {
      return jsonResponse({ error: 'BATCH_QUERY_INVALID' }, 400);
    }
    if (String(rawContract.query.action || '').trim().toUpperCase() !== 'GENERATE') {
      return jsonResponse({ error: 'BATCH_QUERY_ACTION_MISMATCH' }, 400);
    }

    const filters = normaliseInvoiceBatchFilters(rawContract.query, 'GENERATE');
    const sort = normaliseInvoiceBatchSort(rawContract.query, 'GENERATE');
    const selection = normaliseInvoiceBatchSelectionRules(rawContract);
    const snapshotAtUtc = normaliseInvoiceBatchSnapshotUtc(rawContract.query.snapshot_at_utc, new Date().toISOString());
    const commandTokenValue = String(body.command_token || commandToken(req, body)).trim().slice(0, 256);
    if (!commandTokenValue) return jsonResponse({ error: 'GENERATE_COMMAND_TOKEN_REQUIRED' }, 400);

    const selectionContract = {
      contract_version: 'INVOICE_BATCH_SELECTION_V1',
      query: {
        contract_version: 'INVOICE_BATCH_QUERY_V1',
        action: 'GENERATE',
        mode: 'PAGE',
        snapshot_at_utc: snapshotAtUtc,
        allow_early: filters.allow_early,
        display_mode: filters.display_mode,
        filters,
        sort
      },
      selection
    };

    return startCommands(env, req, ctx, user, [{
      command_type: 'GENERATE_SELECTED',
      selection_contract: selectionContract,
      command_token: commandTokenValue
    }], deps, ['DATABASE'], {
      priorityClass: 'INTERACTIVE',
      extendResult: (summary, operationRows) => {
        const root = operationRows[0] || {};
        return {
          root_operation_id: root.operation_id || summary.root_operation_id || null,
          status: root.status || null,
          accepted: root.accepted !== false,
          selection_contract_version: 'INVOICE_BATCH_SELECTION_V1',
          selection_expansion_pending: root.selection_expansion_pending === true,
          estimated_filtered_total: root.estimated_filtered_total ?? null,
          nudge_state: summary.nudge_state
        };
      }
    });
  }

  const rows = Array.isArray(body?.rows) ? body.rows : [];
  const suppliedScopeKeys = Array.isArray(body?.scope_keys) ? body.scope_keys : [];
  if ((!rows.length && !suppliedScopeKeys.length) || rows.length > 500 || suppliedScopeKeys.length > 500) {
    return jsonResponse({ error: 'scope_keys/rows must identify 1..500 candidates' }, 400);
  }
  const rootToken = commandToken(req, body).slice(0, 100);
  const scopeKeys = (suppliedScopeKeys.length ? suppliedScopeKeys : rows.map(row =>
    row.scope_key || row.group_key || row.canonical_command?.scope_key
  )).map(value => String(value || '').trim());
  if (scopeKeys.some(value => !value) || new Set(scopeKeys).size !== scopeKeys.length) {
    return jsonResponse({ error: 'UNIQUE_SCOPE_KEY_REQUIRED' }, 400);
  }
  const expectedRevisions = new Map();
  for (const row of rows) {
    const key = String(row.scope_key || row.group_key || row.canonical_command?.scope_key || '').trim();
    const revision = row.canonical_source_revision
      || row.source_revision_hash
      || row.command_payload?.source_revision
      || row.canonical_command?.source_revision;
    if (key && revision) expectedRevisions.set(key, String(revision));
  }
  for (const [key, revision] of Object.entries(body?.candidate_revisions || {})) {
    if (scopeKeys.includes(key) && revision) expectedRevisions.set(key, String(revision));
  }
  if (scopeKeys.some(key => !expectedRevisions.get(key))) {
    return jsonResponse({ error: 'CANDIDATE_SOURCE_REVISION_REQUIRED' }, 400);
  }

  const candidatesRaw = await deps.rpc('invoice_batch_generate_candidates', {
    p_allow_early: boolValue(body?.allow_early, false),
    p_limit: scopeKeys.length,
    p_scope_keys: scopeKeys
  });
  const candidateByScope = new Map(
    candidateGroupsFromRpc(candidatesRaw)
      .map(candidate => [String(candidate.group_key || candidate.scope_key || ''), candidate])
      .filter(([key]) => key)
  );
  const staleScopes = [];
  const hardBlockedScopes = [];
  const selected = [];
  for (const scopeKey of scopeKeys) {
    const candidate = candidateByScope.get(scopeKey);
    if (!candidate) {
      staleScopes.push({ scope_key: scopeKey, code: 'CANDIDATE_SCOPE_NO_LONGER_AVAILABLE' });
      continue;
    }
    const currentRevision = String(
      candidate.canonical_source_revision
      || candidate.source_revision_hash
      || candidate.command_payload?.source_revision
      || ''
    );
    if (!currentRevision || currentRevision !== expectedRevisions.get(scopeKey)) {
      staleScopes.push({ scope_key: scopeKey, code: 'CANDIDATE_SCOPE_CHANGED' });
      continue;
    }
    const blockerCode = String(candidate.blocker_code || '').trim().toUpperCase();
    if (blockerCode) {
      hardBlockedScopes.push({
        scope_key: scopeKey,
        code: blockerCode,
        detail: candidate.blocker_detail || null
      });
      continue;
    }
    const canonical = candidate.command_payload;
    if (!canonical || typeof canonical !== 'object' || Array.isArray(canonical)) {
      hardBlockedScopes.push({ scope_key: scopeKey, code: 'CANONICAL_COMMAND_REQUIRED' });
      continue;
    }
    selected.push({ scopeKey, candidate, currentRevision });
  }
  if (!selected.length) {
    return jsonResponse({
      ok: false,
      accepted: false,
      accepted_count: 0,
      rejected_count: staleScopes.length + hardBlockedScopes.length,
      stale_scopes: staleScopes,
      hard_blocked_scopes: hardBlockedScopes
    }, 409);
  }

  const commandContextByNo = new Map();
  const commands = await Promise.all(selected.map(async (selection, index) => {
    const canonical = selection.candidate.command_payload
      || selection.candidate.command_ready_payload
      || selection.candidate.canonical_command;
    if (!canonical || typeof canonical !== 'object' || Array.isArray(canonical)) {
      throw Object.assign(new Error('CANONICAL_COMMAND_REQUIRED'), { code: 'CANONICAL_COMMAND_REQUIRED' });
    }
    const commandNo = index + 1;
    commandContextByNo.set(commandNo, selection.scopeKey);
    const memberHash = await sha256Text(`${selection.scopeKey}|${selection.currentRevision}`);
    return generationCommandFromBody(req, {
      canonical_command: canonical,
      allow_early: body.allow_early ?? canonical.allow_early,
      command_token: `${rootToken}:${memberHash}`.slice(0, 200)
    }, canonical.command_type || 'GENERATE_SELECTED');
  }));

  return startCommands(env, req, ctx, user, commands, deps, ['DATABASE'], {
    additionalRejectedCount: staleScopes.length + hardBlockedScopes.length,
    commandContextByNo,
    extendResult: (summary, operationRows) => ({
      enqueued: summary.accepted_count,
      generated: summary.reused_ready_count,
      stale_scopes: staleScopes,
      hard_blocked_scopes: hardBlockedScopes,
      results_invoices: operationRows.map(operation => ({
        scope_key: commandContextByNo.get(Number(operation?.command_no)) || null,
        operation_id: operation?.operation_id || null,
        status: operation?.reused_ready === true ? 'READY' : (operation?.status || (operation?.accepted === false ? 'REJECTED' : 'QUEUED')),
        ok: operation?.accepted !== false,
        error: operation?.error || operation?.terminal_error || null,
        created: operation?.created === true,
        reused_active: operation?.reused_active === true,
        reused_ready: operation?.reused_ready === true
      })),
      invoice_ids: [...new Set(operationRows.flatMap(operation => operation?.created_invoice_ids || operation?.invoice_ids || []).filter(id => UUID_PATTERN.test(String(id))))]
    })
  });
}

async function handleBatchIssueConfirm(env, req, ctx, user, deps) {
  const body = await parseBody(req) || {};

  if (body.selection_contract && typeof body.selection_contract === 'object' && !Array.isArray(body.selection_contract)) {
    const rawContract = body.selection_contract;
    if (rawContract.contract_version !== 'INVOICE_BATCH_SELECTION_V1') {
      return jsonResponse({ error: 'BATCH_SELECTION_CONTRACT_INVALID' }, 400);
    }
    if (!rawContract.query || typeof rawContract.query !== 'object' || Array.isArray(rawContract.query)) {
      return jsonResponse({ error: 'BATCH_QUERY_INVALID' }, 400);
    }
    if (String(rawContract.query.action || '').trim().toUpperCase() !== 'ISSUE') {
      return jsonResponse({ error: 'BATCH_QUERY_ACTION_MISMATCH' }, 400);
    }

    const filters = normaliseInvoiceBatchFilters(rawContract.query, 'ISSUE');
    const sort = normaliseInvoiceBatchSort(rawContract.query, 'ISSUE');
    const selection = normaliseInvoiceBatchSelectionRules(rawContract);
    const snapshotAtUtc = normaliseInvoiceBatchSnapshotUtc(rawContract.query.snapshot_at_utc, new Date().toISOString());
    const deliver = boolValue(body.deliver ?? body.send_email, false);
    const commandTokenValue = String(body.command_token || commandToken(req, body)).trim().slice(0, 256);
    if (!commandTokenValue) return jsonResponse({ error: 'ISSUE_COMMAND_TOKEN_REQUIRED' }, 400);

    let deliveryRequestToken = null;
    let deliveryIntent = { deliver: false };
    if (deliver) {
      deliveryRequestToken = String(body.delivery_request_token || '').trim();
      if (!deliveryRequestToken) return jsonResponse({ error: 'DELIVERY_REQUEST_TOKEN_REQUIRED' }, 400);
      if (deliveryRequestToken.length > 256) return jsonResponse({ error: 'DELIVERY_REQUEST_TOKEN_INVALID' }, 400);
      const rawIntent = body.delivery_intent && typeof body.delivery_intent === 'object' && !Array.isArray(body.delivery_intent)
        ? body.delivery_intent
        : body;
      deliveryIntent = {
        recipient_set: canonicalEmailArray(rawIntent.recipient_set || rawIntent.to || []),
        cc: canonicalEmailArray(rawIntent.cc || []),
        bcc: canonicalEmailArray(rawIntent.bcc || []),
        delivery_policy: canonicalDeliveryPolicy(rawIntent.delivery_policy),
        template_version: rawIntent.template_version || 'INVOICE_EMAIL_V1'
      };
    }

    const selectionContract = {
      contract_version: 'INVOICE_BATCH_SELECTION_V1',
      query: {
        contract_version: 'INVOICE_BATCH_QUERY_V1',
        action: 'ISSUE',
        mode: 'PAGE',
        snapshot_at_utc: snapshotAtUtc,
        allow_early: filters.allow_early,
        display_mode: filters.display_mode,
        filters,
        sort
      },
      selection
    };

    return startCommands(env, req, ctx, user, [{
      command_type: 'ISSUE_INVOICES',
      selection_contract: selectionContract,
      deliver,
      delivery_intent: deliveryIntent,
      command_token: commandTokenValue,
      ...(deliveryRequestToken ? { delivery_request_token: deliveryRequestToken } : {})
    }], deps, ['DATABASE'], {
      priorityClass: 'INTERACTIVE',
      extendResult: (summary, operationRows) => {
        const root = operationRows[0] || {};
        return {
          root_operation_id: root.operation_id || summary.root_operation_id || null,
          status: root.status || null,
          accepted: root.accepted !== false,
          selection_contract_version: 'INVOICE_BATCH_SELECTION_V1',
          selection_expansion_pending: root.selection_expansion_pending === true,
          estimated_filtered_total: root.estimated_filtered_total ?? null,
          delivery_requested: deliver,
          nudge_state: summary.nudge_state
        };
      }
    });
  }

  const rows = Array.isArray(body.rows) ? body.rows : [];
  const invoiceIds = canonicalUuidArray(body.invoice_ids || rows.map(row => row.invoice_id));
  const deliver = boolValue(body.deliver ?? body.send_email, false);
  const policy = deliver ? canonicalDeliveryPolicy(body.delivery_policy) : null;
  const requestToken = commandToken(req, body);
  const deliveryRequestToken = deliver
    ? String(body.delivery_request_token || requestToken).slice(0, 200)
    : null;
  const expectedRevisions = {
    ...Object.fromEntries(
      Object.entries(body.expected_revisions || {})
        .filter(([invoiceId, revision]) => UUID_PATTERN.test(invoiceId) && revision != null)
        .map(([invoiceId, revision]) => [invoiceId.toLowerCase(), String(revision)])
    ),
    ...Object.fromEntries(
      rows
        .filter(row => row?.invoice_id && row?.document_revision != null)
        .map(row => [String(row.invoice_id).toLowerCase(), String(row.document_revision)])
    )
  };
  if (invoiceIds.some(invoiceId => !expectedRevisions[invoiceId])) {
    return jsonResponse({ error: 'EXPECTED_INVOICE_REVISION_REQUIRED' }, 400);
  }
  const command = {
    command_type: 'ISSUE_INVOICES',
    invoice_ids: invoiceIds,
    expected_revisions: expectedRevisions,
    allow_early: boolValue(body.allow_early, false),
    deliver,
    command_token: requestToken,
    delivery_intent: deliver ? {
      recipient_set: canonicalEmailArray(body.recipient_set || body.to || []),
      cc: canonicalEmailArray(body.cc || []),
      bcc: canonicalEmailArray(body.bcc || []),
      delivery_policy: policy,
      template_version: body.template_version || 'INVOICE_EMAIL_V1',
      delivery_request_token: deliveryRequestToken
    } : { deliver: false }
  };
  return startCommands(env, req, ctx, user, [command], deps, ['DATABASE','DOCUMENT'], {
    extendResult: (summary, operationRows) => {
      const root = operationRows[0] || {};
      const rootStatus = root.reused_ready === true ? 'ISSUED' : (root.status || (root.accepted === false ? 'REJECTED' : 'QUEUED'));
      return {
        enqueued: summary.accepted_count,
        invoice_results_enriched: invoiceIds.map(invoiceId => ({
          invoice_id: invoiceId,
          operation_id: root.operation_id || null,
          status: rootStatus,
          ok: root.accepted !== false,
          error: root.error || root.terminal_error || null,
          legal_issue_state: root.result?.legal_issue_state || root.legal_issue_state || (root.reused_ready === true ? 'ISSUED' : 'IN_PROGRESS'),
          delivery_state: deliver ? (root.result?.delivery_state || root.delivery_state || 'IN_PROGRESS') : 'NOT_REQUESTED'
        })),
        invoice_results: [],
        email_outbox: [],
        delivery_requested: deliver
      };
    }
  });
}

async function handleViewDocument(env, req, ctx, user, deps, entityType, entityId) {
  if (!UUID_PATTERN.test(entityId)) return jsonResponse({ error: 'INVALID_ENTITY_ID' }, 400);
  const body = req.method === 'POST' ? (await parseBody(req) || {}) : {};
  const serviceHeaders = {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
  };
  const redactVersion = version => {
    if (!version) return null;
    const { r2_key, ...safeVersion } = version;
    return safeVersion;
  };
  const loadReadyVersion = async (versionId, purpose) => {
    const query = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_document_versions`);
    if (versionId) query.searchParams.set('id', `eq.${versionId}`);
    query.searchParams.set('entity_type', `eq.${entityType}`);
    query.searchParams.set('entity_id', `eq.${entityId}`);
    query.searchParams.set('purpose', `eq.${purpose}`);
    query.searchParams.set('status', 'eq.READY');
    query.searchParams.set('select', 'id,entity_type,entity_id,purpose,source_revision,template_version,r2_key,sha256,size_bytes,page_count,status,ready_at_utc,verified_at_utc');
    query.searchParams.set('order', 'ready_at_utc.desc.nullslast,id.desc');
    query.searchParams.set('limit', '1');
    const response = await fetch(query, { headers: serviceHeaders });
    const rows = await response.json().catch(() => []);
    const version = Array.isArray(rows) ? rows[0] : null;
    return response.ok
      && version?.r2_key
      && version?.sha256
      && Number(version?.size_bytes) > 0
      && Number(version?.page_count) > 0
      ? version
      : null;
  };

  if (entityType === 'INVOICE') {
    const rawDetail = await deps.rpc('invoice_detail_get', {
      p_invoice_id: entityId,
      p_actor_user_id: user.id
    });
    const detailValue = rpcValue(rawDetail);
    const detail = Array.isArray(detailValue) ? detailValue[0] : detailValue;
    const header = detail?.invoice || detail?.header || detail || {};
    const status = String(header.status || detail?.invoice_status || '').toUpperCase();
    if (['ISSUED','PAID'].includes(status)) {
      const issuedVersionId = header.issued_document_version_id || detail?.issued_document_version_id;
      if (!UUID_PATTERN.test(String(issuedVersionId || ''))) {
        return jsonResponse({
          ok: false,
          viewer_state: 'BLOCKED',
          badge_codes: ['ISSUED_DOCUMENT_POINTER_MISSING'],
          error_code: 'ISSUED_DOCUMENT_POINTER_MISSING'
        }, 409);
      }
      const version = await loadReadyVersion(issuedVersionId, 'FINAL_ISSUE');
      return version
        ? jsonResponse({
          ok: true,
          ready: true,
          viewer_state: 'READY',
          purpose: 'FINAL_ISSUE',
          document_version_id: version.id,
          document_version: redactVersion(version)
        })
        : jsonResponse({
          ok: false,
          viewer_state: 'BLOCKED',
          badge_codes: ['ISSUED_DOCUMENT_INTEGRITY_FAILURE'],
          error_code: 'ISSUED_DOCUMENT_INTEGRITY_FAILURE',
          document_version_id: issuedVersionId
        }, 409);
    }
  }

  const purpose = entityType === 'INVOICE' ? 'DRAFT_PREVIEW' : 'TIMESHEET';
  const command = {
    command_type: entityType === 'INVOICE' ? 'VIEW_INVOICE_DOCUMENT' : 'VIEW_TIMESHEET_DOCUMENT',
    [entityType === 'INVOICE' ? 'invoice_id' : 'timesheet_id']: entityId,
    purpose,
    priority_reason: body.priority_reason || 'VIEW_NOW',
    template_version: body.template_version
  };
  const operationsValue = rpcValue(await deps.rpc('invoice_operation_start_batch', {
    p_commands: [command],
    p_actor_user_id: user.id,
    p_now_utc: new Date().toISOString()
  }));
  const operations = Array.isArray(operationsValue) ? operationsValue : [operationsValue];
  const result = operations[0] || {};

  if (result.accepted === true && result.reused_ready === true && result.document_version_id) {
    const version = await loadReadyVersion(result.document_version_id, purpose);
    return version
      ? jsonResponse({
        ok: true,
        ready: true,
        viewer_state: 'READY',
        purpose,
        document_version_id: version.id,
        document_version: redactVersion(version)
      })
      : jsonResponse({
        ok: false,
        viewer_state: 'BLOCKED',
        badge_codes: ['READY_DOCUMENT_IDENTITY_INVALID'],
        error_code: 'READY_DOCUMENT_IDENTITY_INVALID',
        document_version_id: result.document_version_id
      }, 409);
  }

  if (result.accepted === false || result.blocked === true || result.terminal_error) {
    const errorCode = String(result?.terminal_error?.code || result?.error?.code || result?.terminal_error || result?.error || 'DOCUMENT_PREPARATION_BLOCKED').slice(0, 160);
    return jsonResponse({
      ok: false,
      viewer_state: 'BLOCKED',
      status: result.status || 'BLOCKED',
      purpose,
      badge_codes: [errorCode],
      error_code: errorCode,
      operation: result
    }, 409);
  }

  const activeOperations = operations.filter(row => row?.accepted !== false && row?.operation_id);
  const nudge = activeOperations.length
    ? await nudgeInvoiceOperations(env, activeOperations, {
      ctx,
      rpc: deps.rpc,
      lanes: ['DATABASE','DOCUMENT'],
      priorityClass: 'VIEW_NOW'
    })
    : { scheduled: false, code: 'NO_ACTIVE_WORK' };
  return jsonResponse({
    ok: true,
    accepted: true,
    ready: false,
    viewer_state: 'PREPARING',
    status: result.status || 'QUEUED',
    operation_id: result.operation_id || null,
    operations,
    purpose,
    status_message: entityType === 'INVOICE' ? 'Preparing preview' : 'Preparing timesheet',
    nudge
  }, 202);
}

async function handleIssueOne(env, req, ctx, user, deps, invoiceId) {
  const body = await parseBody(req) || {};
  const expectedRevision = String(body.expected_revision ?? '').trim();
  if (!/^[1-9][0-9]*$/.test(expectedRevision)) {
    return jsonResponse({ error: 'EXPECTED_INVOICE_REVISION_REQUIRED' }, 400);
  }
  const canonicalInvoiceId = canonicalUuidArray([invoiceId])[0];
  const deliver = boolValue(body.deliver ?? body.send_email, false);
  const requestToken = commandToken(req, body);
  return startCommands(env, req, ctx, user, [{
    command_type: 'ISSUE_INVOICES',
    invoice_ids: [canonicalInvoiceId],
    expected_revisions: { [canonicalInvoiceId]: expectedRevision },
    allow_early: boolValue(body.allow_early, false),
    deliver,
    command_token: requestToken,
    delivery_intent: deliver ? {
      recipient_set: canonicalEmailArray(body.recipient_set || body.to || []),
      cc: canonicalEmailArray(body.cc || []),
      bcc: canonicalEmailArray(body.bcc || []),
      delivery_policy: canonicalDeliveryPolicy(body.delivery_policy),
      template_version: body.template_version || 'INVOICE_EMAIL_V1',
      delivery_request_token: body.delivery_request_token || requestToken
    } : { deliver: false }
  }], deps, ['DATABASE', 'DOCUMENT']);
}

async function handleDeliverOne(env, req, ctx, user, deps, invoiceId) {
  const body = await parseBody(req) || {};
  return startCommands(env, req, ctx, user, [{
    command_type: 'DELIVER_INVOICES',
    invoice_ids: canonicalUuidArray([invoiceId]),
    recipient_set: canonicalEmailArray(body.recipient_set || body.to || []),
    cc: canonicalEmailArray(body.cc || []),
    bcc: canonicalEmailArray(body.bcc || []),
    delivery_policy: canonicalDeliveryPolicy(body.delivery_policy),
    delivery_template_version: body.template_version || 'INVOICE_EMAIL_V1',
    delivery_part_number: Number(body.delivery_part_number || 1),
    delivery_request_token: commandToken(req, body)
  }], deps, ['DATABASE']);
}

async function handleOperationGet(envOrReq, reqOrUser, userOrDeps, depsOrOperationId, maybeOperationId) {
  const hasExplicitEnv = maybeOperationId !== undefined || !(envOrReq && typeof envOrReq.url === 'string');
  const env = hasExplicitEnv ? envOrReq : (userOrDeps?.env || null);
  const req = hasExplicitEnv ? reqOrUser : envOrReq;
  const user = hasExplicitEnv ? userOrDeps : reqOrUser;
  const deps = hasExplicitEnv ? depsOrOperationId : userOrDeps;
  const operationId = hasExplicitEnv ? maybeOperationId : depsOrOperationId;
  const url = new URL(req.url);
  const body = req.method === 'POST' ? (await parseBody(req) || {}) : {};
  const ids = operationId
    ? canonicalUuidArray([operationId])
    : canonicalUuidArray(body.operation_ids || String(url.searchParams.get('operation_ids') || '').split(',').filter(Boolean));
  const mode = String(body.mode || url.searchParams.get('mode') || 'PROGRESS').toUpperCase();
  const categoryRaw = body.result_category || body.category || url.searchParams.get('result_category') || url.searchParams.get('category');
  const resultCategory = categoryRaw ? String(categoryRaw).trim().toUpperCase() : null;
  if (resultCategory && !INVOICE_OPERATION_RESULT_CATEGORIES.includes(resultCategory)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CATEGORY_INVALID');
  }
  const resultCursorToken = body.result_cursor || body.cursor || url.searchParams.get('result_cursor') || url.searchParams.get('cursor');
  const resultLimitRaw = Number(body.result_limit || body.limit || url.searchParams.get('result_limit') || url.searchParams.get('limit') || 100);
  const resultLimit = Number.isFinite(resultLimitRaw) ? Math.max(1, Math.min(100, Math.trunc(resultLimitRaw))) : 100;

  let pageRequest = null;
  let resultAction = String(body.action || url.searchParams.get('action') || '').trim().toUpperCase();
  if (!resultAction) resultAction = ['ISSUED', 'ISSUED_SEND_BLOCKED'].includes(resultCategory) ? 'ISSUE' : 'GENERATE';
  if (resultCategory || resultCursorToken || body.after_selection_key || body.after_chunk_id || url.searchParams.get('after_selection_key') || url.searchParams.get('after_chunk_id')) {
    const category = resultCategory || 'ALL';
    const resultFilterHash = await sha256Text(JSON.stringify({
      operation_ids: ids,
      category
    }));
    let cursorValues = {};
    let cursorSnapshot = new Date().toISOString();
    if (resultCursorToken) {
      if (!env) throw Object.assign(new Error('BATCH_CURSOR_ENV_REQUIRED'), { code: 'BATCH_CURSOR_ENV_REQUIRED' });
      const decoded = await decodeInvoiceBatchCursor(env, resultCursorToken, {
        action: resultAction,
        filter_hash: resultFilterHash,
        result_category: category,
        sort: { group_preset: 'WEEK_CLIENT_CANDIDATE', sort_key: 'WEEK_ENDING_DATE', sort_direction: 'ASC' }
      });
      cursorValues = decoded.cursor || {};
      cursorSnapshot = decoded.snapshot_at_utc || cursorSnapshot;
    }
    pageRequest = {
      category,
      limit: resultLimit,
      after_selection_key: cursorValues.after_selection_key
        || body.after_selection_key
        || url.searchParams.get('after_selection_key')
        || null,
      after_chunk_id: cursorValues.after_chunk_id
        || body.after_chunk_id
        || url.searchParams.get('after_chunk_id')
        || null,
      _cursor_snapshot_at_utc: cursorSnapshot,
      _cursor_filter_hash: resultFilterHash,
      _cursor_action: resultAction
    };
  }

  const result = await deps.rpc('invoice_operation_get', {
    p_operation_ids: ids,
    p_actor_user_id: user.id,
    p_mode: mode,
    p_page_request: pageRequest ? {
      category: pageRequest.category,
      limit: pageRequest.limit,
      after_selection_key: pageRequest.after_selection_key,
      after_chunk_id: pageRequest.after_chunk_id
    } : null
  });
  const value = rpcValue(result);
  const operations = Array.isArray(value) ? value : (Array.isArray(value?.operations) ? value.operations : []);
  const resultPage = value && !Array.isArray(value) ? value.result_page || null : null;
  let signedNextResultCursor = null;
  if (resultPage?.has_more && resultPage?.next_cursor_values && pageRequest) {
    if (!env) throw Object.assign(new Error('BATCH_CURSOR_ENV_REQUIRED'), { code: 'BATCH_CURSOR_ENV_REQUIRED' });
    signedNextResultCursor = await encodeInvoiceBatchCursor(env, {
      action: pageRequest._cursor_action,
      snapshot_at_utc: pageRequest._cursor_snapshot_at_utc,
      filter_hash: pageRequest._cursor_filter_hash,
      sort: { group_preset: 'WEEK_CLIENT_CANDIDATE', sort_key: 'WEEK_ENDING_DATE', sort_direction: 'ASC' },
      result_category: pageRequest.category,
      next_cursor_values: resultPage.next_cursor_values
    });
  }

  return jsonResponse({
    ok: true,
    operations,
    ...(resultPage ? {
      result_page: resultPage,
      signed_next_result_cursor: signedNextResultCursor
    } : {})
  });
}

function normaliseInvoiceOperationControlAction(raw) {
  const allowed = new Set(['RETRY','CANCEL','RESCHEDULE','RAISE_PRIORITY']);
  const action = String(raw?.action || '').trim().toUpperCase();
  const operationId = String(raw?.operation_id || '').trim().toLowerCase();
  if (!allowed.has(action) || !UUID_PATTERN.test(operationId)) {
    throw Object.assign(new Error('OPERATION_CONTROL_ACTION_INVALID'), { code: 'OPERATION_CONTROL_ACTION_INVALID' });
  }
  const item = { operation_id: operationId, action };
  if (action === 'RETRY' && raw.retry_chunk_id != null) {
    const retryChunkId = String(raw.retry_chunk_id).trim().toLowerCase();
    if (!UUID_PATTERN.test(retryChunkId)) throw Object.assign(new Error('RETRY_CHUNK_ID_INVALID'), { code: 'RETRY_CHUNK_ID_INVALID' });
    item.retry_chunk_id = retryChunkId;
  }
  if (action === 'RESCHEDULE') {
    const timestamp = new Date(raw.run_after_utc || raw.scheduled_for_utc || raw.scheduledForUtc || '');
    if (!Number.isFinite(timestamp.getTime()) || timestamp.getTime() <= Date.now()) {
      throw Object.assign(new Error('RESCHEDULE_TIMESTAMP_INVALID'), { code: 'RESCHEDULE_TIMESTAMP_INVALID' });
    }
    item.run_after_utc = timestamp.toISOString();
  }
  return item;
}

function controlResultsReleasedRunnableWork(value) {
  const rows = Array.isArray(value) ? value : (value ? [value] : []);
  return rows.some(row =>
    row?.accepted === true
    && ['QUEUED','WAITING','RETRY_WAIT','RUNNING'].includes(String(row?.status || '').toUpperCase())
  );
}

async function handleOperationControl(env, req, ctx, user, deps) {
  const body = await parseBody(req);
  const actions = Array.isArray(body?.actions) ? body.actions : [];
  if (!actions.length || actions.length > 100) return jsonResponse({ error: 'actions[] must contain 1..100 items' }, 400);
  const safeActions = actions.map(normaliseInvoiceOperationControlAction);
  const operations = rpcValue(await deps.rpc('invoice_operation_control_batch', { p_actions: safeActions, p_actor_user_id: user.id, p_now_utc: new Date().toISOString() }));
  if (controlResultsReleasedRunnableWork(operations)) {
    await nudgeInvoiceOperations(env, operations, { ctx, rpc: deps.rpc, lanes: ['ALL'] });
  }
  return jsonResponse({ ok: true, results: operations });
}
function normaliseUnifiedOutboxPayload(value) {
  const unwrapped = rpcValue(value);
  if (unwrapped && typeof unwrapped === 'object' && !Array.isArray(unwrapped)) {
    const items = Array.isArray(unwrapped.items)
      ? unwrapped.items
      : (Array.isArray(unwrapped.rows) ? unwrapped.rows : []);
    const total = Number(
      unwrapped.total_count
      ?? unwrapped.total
      ?? unwrapped.count
      ?? items.length
    );
    return {
      ...unwrapped,
      items,
      total_count: Number.isFinite(total) ? total : items.length
    };
  }
  const items = Array.isArray(unwrapped) ? unwrapped : [];
  return { items, total_count: items.length };
}

function invoiceOperationOutboxRow(row) {
  const progress = row.progress_json && typeof row.progress_json === 'object' ? row.progress_json : {};
  const result = row.result_json && typeof row.result_json === 'object' ? row.result_json : {};
  const authoritativeAttempts = Number(result.attempt_summary?.attempt_count);
  return { channel: 'INVOICE', outbox_id: row.id, id: row.id, outbox_type: row.operation_type, type: row.operation_type, entity_type: row.entity_type || null, entity_id: row.entity_id || null, status: row.status, queue_state: row.status, phase: row.phase, legal_issue_state: result.legal_issue_state || progress.legal_issue_state || 'NOT_REQUESTED', delivery_state: result.delivery_state || progress.delivery_state || 'NOT_REQUESTED', requires_user_action: row.requires_user_action === true, progress_summary: { completed_units: Number(row.completed_units || 0), total_units: Number(row.total_units || 0), failed_units: Number(row.failed_units || 0), pages_complete: Number(progress.pages_complete || 0), pages_total: Number(progress.pages_total || 0), status_message: String(progress.status_message || '').slice(0, 200) || null }, retry_summary: { run_after_utc: row.run_after_utc || null, attempt_count: Number.isSafeInteger(authoritativeAttempts) ? authoritativeAttempts : null, attempt_detail_available: true }, error_code: String(row.error_json?.code || row.error_json?.error_code || '').slice(0, 120) || null, created_at_utc: row.created_at_utc, scheduled_for_utc: row.run_after_utc || null, effective_ready_at_utc: row.run_after_utc || row.created_at_utc, change_seq: Number(row.change_seq || 0), parent_operation_id: row.parent_operation_id || null };
}

function compareUnifiedOutboxRows(left, right, sortBy, sortDir) {
  const key = sortBy === 'channel'
    ? 'channel'
    : (sortBy === 'status' ? 'status' : sortBy);
  const a = left?.[key] ?? '';
  const b = right?.[key] ?? '';
  const numericA = /_at_utc$/.test(key) ? Date.parse(a) : NaN;
  const numericB = /_at_utc$/.test(key) ? Date.parse(b) : NaN;
  const compared = Number.isFinite(numericA) && Number.isFinite(numericB)
    ? numericA - numericB
    : String(a).localeCompare(String(b));
  if (compared !== 0) return sortDir === 'asc' ? compared : -compared;
  return String(left?.outbox_id || left?.id || '').localeCompare(
    String(right?.outbox_id || right?.id || '')
  ) * (sortDir === 'asc' ? 1 : -1);
}

function encodeOutboxCursorPart(value) {
  const bytes = typeof value === 'string' ? new TextEncoder().encode(value) : value;
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function decodeOutboxCursorPart(value) {
  const padded = String(value || '').replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(String(value || '').length / 4) * 4, '=');
  const binary = atob(padded);
  return Uint8Array.from(binary, character => character.charCodeAt(0));
}

async function sha256Hex(value) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(String(value)));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

async function signOutboxCursor(secret, encodedPayload) {
  if (!secret || String(secret).length < 32) throw Object.assign(new Error('OUTBOX_CURSOR_SECRET_INVALID'), { code: 'OUTBOX_CURSOR_SECRET_INVALID' });
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(String(secret)), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const signature = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(`invoice-outbox-cursor-v1.${encodedPayload}`));
  return encodeOutboxCursorPart(new Uint8Array(signature));
}

async function encodeUnifiedOutboxCursor(env, payload) {
  const encodedPayload = encodeOutboxCursorPart(new TextEncoder().encode(JSON.stringify(payload)));
  return `${encodedPayload}.${await signOutboxCursor(env.SESSION_TOKEN_SECRET, encodedPayload)}`;
}

async function decodeUnifiedOutboxCursor(env, value) {
  const parts = String(value || '').split('.');
  if (parts.length !== 2 || parts.some(part => !part)) throw Object.assign(new Error('OUTBOX_CURSOR_INVALID'), { code: 'OUTBOX_CURSOR_INVALID' });
  const expected = decodeOutboxCursorPart(await signOutboxCursor(env.SESSION_TOKEN_SECRET, parts[0]));
  const actual = decodeOutboxCursorPart(parts[1]);
  if (expected.byteLength !== actual.byteLength) throw Object.assign(new Error('OUTBOX_CURSOR_INVALID'), { code: 'OUTBOX_CURSOR_INVALID' });
  let difference = 0;
  for (let index = 0; index < expected.byteLength; index += 1) difference |= expected[index] ^ actual[index];
  if (difference !== 0) throw Object.assign(new Error('OUTBOX_CURSOR_INVALID'), { code: 'OUTBOX_CURSOR_INVALID' });
  let payload;
  try {
    payload = JSON.parse(new TextDecoder().decode(decodeOutboxCursorPart(parts[0])));
  } catch {
    throw Object.assign(new Error('OUTBOX_CURSOR_INVALID'), { code: 'OUTBOX_CURSOR_INVALID' });
  }
  if (
    payload?.v !== 1
    || payload?.sort !== 'created_at_utc_desc_channel_rank_id_desc'
    || !Number.isFinite(Date.parse(payload?.snapshot_at_utc || ''))
  ) throw Object.assign(new Error('OUTBOX_CURSOR_INVALID'), { code: 'OUTBOX_CURSOR_INVALID' });
  return payload;
}

function legacyQueueState(row, nowMs = Date.now()) {
  if (row.read_at) return 'READ';
  if (row.delivered_at) return 'DELIVERED';
  if (row.sent_at) return 'SENT';
  if (String(row.status || '').toUpperCase() === 'FAILED' || row.failed_at) return 'FAILED';
  if (String(row.status || '').toUpperCase() === 'QUEUED') {
    const readyAt = Date.parse(row.next_attempt_at_utc || row.scheduled_for_utc || row.created_at_utc || '');
    return Number.isFinite(readyAt) && readyAt > nowMs ? 'SCHEDULED' : 'QUEUED';
  }
  return String(row.status || '').toUpperCase();
}

function legacyOutboxCursorRow(row) {
  const queueState = legacyQueueState(row);
  return {
    channel: String(row.channel || '').toUpperCase(),
    outbox_id: row.outbox_id,
    id: row.outbox_id,
    outbox_type: row.outbox_type || null,
    type: row.outbox_type || null,
    status: row.status || null,
    queue_state: queueState,
    delivery_status: row.delivery_status || null,
    created_at_utc: row.created_at_utc,
    sent_at: row.sent_at || null,
    delivered_at: row.delivered_at || null,
    read_at: row.read_at || null,
    failed_at: row.failed_at || null,
    to_address: String(row.to_address || '').slice(0, 320) || null,
    subject: String(row.subject || '').slice(0, 500) || null,
    reference: String(row.reference || '').slice(0, 240) || null,
    provider_message_id: String(row.provider_message_id || '').slice(0, 240) || null,
    recipient_kind: row.recipient_kind || null,
    recipient_id: row.recipient_id || null,
    context_kind: row.context_kind || null,
    context_id: row.context_id || null,
    scheduled_for_utc: row.scheduled_for_utc || null,
    effective_ready_at_utc: row.next_attempt_at_utc || row.scheduled_for_utc || row.created_at_utc,
    error_code: String(row.last_error || '').slice(0, 120) || null
  };
}

function outboxChannelRank(row) {
  const channel = String(row?.channel || '').toUpperCase();
  return channel === 'EMAIL' ? 0 : (channel === 'SMS' ? 1 : (channel === 'INVOICE' ? 2 : 9));
}

function compareCursorOutboxRows(left, right) {
  const timeDifference = Date.parse(right?.created_at_utc || '') - Date.parse(left?.created_at_utc || '');
  if (timeDifference !== 0) return timeDifference;
  const rankDifference = outboxChannelRank(left) - outboxChannelRank(right);
  if (rankDifference !== 0) return rankDifference;
  return String(right?.outbox_id || right?.id || '').localeCompare(String(left?.outbox_id || left?.id || ''));
}

function outboxCursorKeysetExpression(cursor, idColumn) {
  if (!cursor) return null;
  if (!Number.isFinite(Date.parse(cursor.created_at_utc || '')) || !UUID_PATTERN.test(String(cursor.id || ''))) {
    throw Object.assign(new Error('OUTBOX_CURSOR_INVALID'), { code: 'OUTBOX_CURSOR_INVALID' });
  }
  return `or(created_at_utc.lt.${cursor.created_at_utc},and(created_at_utc.eq.${cursor.created_at_utc},${idColumn}.lt.${cursor.id}))`;
}

function parseExactContentRange(response, fallback) {
  const parsed = Number((response.headers.get('content-range') || '').split('/')[1]);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function legacyOutboxSearchExpression(search) {
  if (!search) return null;
  if (UUID_PATTERN.test(search)) return `or(outbox_id.eq.${search},context_id.eq.${search},recipient_id.eq.${search})`;
  if (!/^[a-z0-9 _-]{1,80}$/i.test(search)) throw Object.assign(new Error('INVALID_OUTBOX_SEARCH'), { code: 'INVALID_OUTBOX_SEARCH' });
  const term = search.replace(/\\/g, '\\\\').replace(/_/g, '\\_').replace(/%/g, '\\%').replace(/ +/g, '%');
  return `or(to_address.ilike.*${term}*,subject.ilike.*${term}*,reference.ilike.*${term}*,provider_message_id.ilike.*${term}*)`;
}

function legacyOutboxQueueStateExpression(queueState, snapshotAt) {
  if (!queueState) return null;
  if (queueState === 'READ') return 'read_at.not.is.null';
  if (queueState === 'DELIVERED') return 'and(read_at.is.null,delivered_at.not.is.null)';
  if (queueState === 'SENT') return 'and(read_at.is.null,delivered_at.is.null,sent_at.not.is.null)';
  if (queueState === 'FAILED') return 'and(read_at.is.null,delivered_at.is.null,sent_at.is.null,or(status.eq.FAILED,failed_at.not.is.null))';
  if (queueState === 'SCHEDULED') {
    return `and(read_at.is.null,delivered_at.is.null,sent_at.is.null,failed_at.is.null,status.eq.QUEUED,or(next_attempt_at_utc.gt.${snapshotAt},and(next_attempt_at_utc.is.null,scheduled_for_utc.gt.${snapshotAt}),and(next_attempt_at_utc.is.null,scheduled_for_utc.is.null,created_at_utc.gt.${snapshotAt})))`;
  }
  if (queueState === 'QUEUED') {
    return `and(read_at.is.null,delivered_at.is.null,sent_at.is.null,failed_at.is.null,status.eq.QUEUED,or(next_attempt_at_utc.lte.${snapshotAt},and(next_attempt_at_utc.is.null,scheduled_for_utc.lte.${snapshotAt}),and(next_attempt_at_utc.is.null,scheduled_for_utc.is.null,created_at_utc.lte.${snapshotAt})))`;
  }
  if (queueState === 'RUNNING' || queueState === 'ACTION_REQUIRED') {
    return 'outbox_id.is.null';
  }
  throw Object.assign(new Error('INVALID_OUTBOX_QUEUE_STATE'), { code: 'INVALID_OUTBOX_QUEUE_STATE' });
}

function normaliseInvoiceOutboxQueueState(queueState, snapshotAt) {
  const state = String(queueState || '').trim().toUpperCase();
  if (!state) return { expression: null, requiresAction: null, semantics: null };
  if (state === 'QUEUED') {
    return {
      expression: `or(status.in.(QUEUED,WAITING),and(status.eq.RETRY_WAIT,run_after_utc.lte.${snapshotAt}))`,
      requiresAction: null,
      semantics: 'QUEUED_OR_WAITING_OR_DUE_RETRY'
    };
  }
  if (state === 'RUNNING') {
    return { expression: 'status.eq.RUNNING', requiresAction: null, semantics: 'RUNNING' };
  }
  if (state === 'SCHEDULED') {
    return {
      expression: `and(status.eq.RETRY_WAIT,run_after_utc.gt.${snapshotAt})`,
      requiresAction: null,
      semantics: 'FUTURE_RETRY'
    };
  }
  if (state === 'FAILED') {
    return { expression: 'status.in.(FAILED,DEAD_LETTER,BLOCKED)', requiresAction: null, semantics: 'FAILED_OR_BLOCKED' };
  }
  if (state === 'ACTION_REQUIRED') {
    return { expression: null, requiresAction: 'true', semantics: 'REQUIRES_USER_ACTION' };
  }
  if (['SENT','DELIVERED','READ'].includes(state)) {
    return { expression: 'id.is.null', requiresAction: null, semantics: 'NOT_APPLICABLE_TO_INVOICE_OPERATIONS' };
  }
  throw Object.assign(new Error('INVALID_OUTBOX_QUEUE_STATE'), { code: 'INVALID_OUTBOX_QUEUE_STATE' });
}

function applyPostgrestAndExpressions(query, expressions) {
  const filtered = expressions.filter(Boolean);
  if (filtered.length) query.searchParams.set('and', `(${filtered.join(',')})`);
}

function applyLegacyOutboxFilters(query, { status }) {
  if (status && /^[A-Z_]+$/.test(status)) query.searchParams.set('status', `eq.${status}`);
}

async function loadUnifiedOutboxCursorPage(env, {
  limit,
  status,
  queueState,
  search,
  operationType,
  entityId,
  requiresAction,
  cursorPayload
}) {
  const snapshotAt = cursorPayload?.snapshot_at_utc || new Date().toISOString();
  const invoiceQueue = normaliseInvoiceOutboxQueueState(queueState, snapshotAt);
  const perSourceLimit = limit + 1;
  const invoiceQuery = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_operations`);
  invoiceQuery.searchParams.set('select', 'id,operation_type,entity_type,entity_id,status,phase,priority,total_units,completed_units,failed_units,progress_json,result_json,error_json,requires_user_action,change_seq,created_at_utc,updated_at_utc,run_after_utc,parent_operation_id');
  invoiceQuery.searchParams.set('created_at_utc', `lte.${snapshotAt}`);
  if (status && /^[A-Z_]+$/.test(status)) invoiceQuery.searchParams.set('status', `eq.${status}`);
  if (operationType && /^[A-Z_]+$/.test(operationType)) invoiceQuery.searchParams.set('operation_type', `eq.${operationType}`);
  if (entityId) invoiceQuery.searchParams.set('entity_id', `eq.${entityId}`);
  const invoiceRequiresAction = invoiceQueue.requiresAction ?? requiresAction;
  if (invoiceRequiresAction === 'true' || invoiceRequiresAction === 'false') invoiceQuery.searchParams.set('requires_user_action', `eq.${invoiceRequiresAction}`);
  const invoiceExpressions = [];
  if (search) {
    if (UUID_PATTERN.test(search)) invoiceExpressions.push(`or(id.eq.${search},entity_id.eq.${search})`);
    else {
      const term = search.replace(/\\/g, '\\\\').replace(/_/g, '\\_').replace(/%/g, '\\%').replace(/ +/g, '%');
      invoiceExpressions.push(`or(operation_type.ilike.*${term}*,phase.ilike.*${term}*)`);
    }
  }
  invoiceExpressions.push(invoiceQueue.expression);
  invoiceExpressions.push(outboxCursorKeysetExpression(cursorPayload?.invoice, 'id'));
  applyPostgrestAndExpressions(invoiceQuery, invoiceExpressions);
  invoiceQuery.searchParams.set('order', 'created_at_utc.desc,id.desc');
  invoiceQuery.searchParams.set('limit', String(perSourceLimit));

  const legacyQuery = new URL(`${env.SUPABASE_URL}/rest/v1/v_outbox_unified`);
  legacyQuery.searchParams.set('select', 'channel,outbox_id,outbox_type,status,delivery_status,created_at_utc,sent_at,delivered_at,read_at,failed_at,to_address,subject,reference,provider_message_id,last_error,recipient_kind,recipient_id,context_kind,context_id,scheduled_for_utc,next_attempt_at_utc');
  legacyQuery.searchParams.set('created_at_utc', `lte.${snapshotAt}`);
  applyLegacyOutboxFilters(legacyQuery, { status });
  applyPostgrestAndExpressions(legacyQuery, [
    legacyOutboxSearchExpression(search),
    legacyOutboxQueueStateExpression(queueState, snapshotAt),
    outboxCursorKeysetExpression(cursorPayload?.legacy, 'outbox_id')
  ]);
  legacyQuery.searchParams.set('order', 'created_at_utc.desc,outbox_id.desc');
  legacyQuery.searchParams.set('limit', String(perSourceLimit));

  const headers = { apikey: env.SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, prefer: 'count=exact' };
  const [invoiceResponse, legacyResponse] = await Promise.all([
    fetch(invoiceQuery, { headers }),
    fetch(legacyQuery, { headers })
  ]);
  const [invoiceRows, legacyRows] = await Promise.all([
    invoiceResponse.json().catch(() => []),
    legacyResponse.json().catch(() => [])
  ]);
  if (!invoiceResponse.ok || !Array.isArray(invoiceRows) || !legacyResponse.ok || !Array.isArray(legacyRows)) {
    throw Object.assign(new Error('UNIFIED_OUTBOX_LIST_FAILED'), { code: 'UNIFIED_OUTBOX_LIST_FAILED' });
  }
  return {
    snapshot_at_utc: snapshotAt,
    invoice_rows: invoiceRows.map(invoiceOperationOutboxRow),
    legacy_rows: legacyRows.map(legacyOutboxCursorRow),
    invoice_total: cursorPayload?.totals?.invoice ?? parseExactContentRange(invoiceResponse, invoiceRows.length),
    legacy_total: cursorPayload?.totals?.legacy ?? parseExactContentRange(legacyResponse, legacyRows.length),
    per_source_limit: perSourceLimit
  };
}

async function handleUnifiedOutboxCursorList(env, {
  limit,
  offset,
  status,
  queueState,
  search,
  sortBy,
  sortDir,
  operationType,
  entityId,
  requiresAction,
  cursorToken
}) {
  if (offset !== 0) return jsonResponse({ error: 'UNIFIED_OUTBOX_USE_CURSOR' }, 400);
  if (sortBy !== 'created_at_utc' || sortDir !== 'desc') return jsonResponse({ error: 'UNIFIED_OUTBOX_SORT_UNSUPPORTED' }, 400);
  if (status && !/^[A-Z_]+$/.test(status)) return jsonResponse({ error: 'INVALID_OUTBOX_STATUS' }, 400);
  if (queueState && !new Set(['SCHEDULED','QUEUED','RUNNING','ACTION_REQUIRED','SENT','DELIVERED','READ','FAILED']).has(queueState)) return jsonResponse({ error: 'INVALID_OUTBOX_QUEUE_STATE' }, 400);
  if (entityId && !UUID_PATTERN.test(entityId)) return jsonResponse({ error: 'INVALID_ENTITY_ID' }, 400);
  if (search && !UUID_PATTERN.test(search) && !/^[a-z0-9 _-]{1,80}$/i.test(search)) return jsonResponse({ error: 'INVALID_OUTBOX_SEARCH' }, 400);

  const filterIdentity = JSON.stringify({ status, queue_state: queueState, search, operation_type: operationType, entity_id: entityId, requires_user_action: requiresAction });
  const filtersHash = await sha256Hex(filterIdentity);
  let cursorPayload = null;
  if (cursorToken) {
    cursorPayload = await decodeUnifiedOutboxCursor(env, cursorToken);
    if (cursorPayload.filters_hash !== filtersHash) return jsonResponse({ error: 'OUTBOX_CURSOR_FILTER_MISMATCH' }, 400);
  }
  const page = await loadUnifiedOutboxCursorPage(env, { limit, status, queueState, search, operationType, entityId, requiresAction, cursorPayload });
  const tagged = [
    ...page.legacy_rows.map(row => ({ source: 'legacy', row })),
    ...page.invoice_rows.map(row => ({ source: 'invoice', row }))
  ].sort((left, right) => compareCursorOutboxRows(left.row, right.row));
  const consumed = tagged.slice(0, limit);
  const items = consumed.map(entry => entry.row);
  const lastLegacy = [...consumed].reverse().find(entry => entry.source === 'legacy')?.row;
  const lastInvoice = [...consumed].reverse().find(entry => entry.source === 'invoice')?.row;
  const consumedLegacy = consumed.filter(entry => entry.source === 'legacy').length;
  const consumedInvoice = consumed.filter(entry => entry.source === 'invoice').length;
  const legacyHasMore = page.legacy_rows.length > consumedLegacy;
  const invoiceHasMore = page.invoice_rows.length > consumedInvoice;
  const hasMore = legacyHasMore || invoiceHasMore;
  const nextPayload = {
    v: 1,
    snapshot_at_utc: page.snapshot_at_utc,
    filters_hash: filtersHash,
    sort: 'created_at_utc_desc_channel_rank_id_desc',
    legacy: lastLegacy ? { created_at_utc: lastLegacy.created_at_utc, id: lastLegacy.outbox_id || lastLegacy.id } : (cursorPayload?.legacy || null),
    invoice: lastInvoice ? { created_at_utc: lastInvoice.created_at_utc, id: lastInvoice.outbox_id || lastInvoice.id } : (cursorPayload?.invoice || null),
    totals: { legacy: page.legacy_total, invoice: page.invoice_total }
  };
  return jsonResponse({
    ok: true,
    channel: null,
    total_count: Number(page.legacy_total || 0) + Number(page.invoice_total || 0),
    source_totals: { legacy: page.legacy_total, invoice: page.invoice_total },
    limit,
    returned_count: items.length,
    items,
    has_more: hasMore,
    source_has_more: { legacy: legacyHasMore, invoice: invoiceHasMore },
    next_cursor: hasMore ? await encodeUnifiedOutboxCursor(env, nextPayload) : null,
    snapshot_at_utc: page.snapshot_at_utc
  });
}

async function handleInvoiceOutboxList(env, req, deps) {
  const url = new URL(req.url);
  const limit = Math.max(1, Math.min(500, Math.trunc(Number(url.searchParams.get('limit')) || 50)));
  const offset = Math.max(0, Math.trunc(Number(url.searchParams.get('offset')) || 0));
  const queueState = String(url.searchParams.get('queue_state') || '').trim().toUpperCase();
  const status = String(url.searchParams.get('status') || '').trim().toUpperCase();
  const channel = String(url.searchParams.get('channel') || '').trim().toUpperCase();
  const search = String(url.searchParams.get('search') || '').trim().toLowerCase();
  const sortBy = String(url.searchParams.get('sort_by') || 'created_at_utc').trim();
  const sortDir = String(url.searchParams.get('sort_dir') || 'desc').trim().toLowerCase();
  if (!new Set(['created_at_utc','scheduled_for_utc','effective_ready_at_utc','status','channel']).has(sortBy) || !['asc','desc'].includes(sortDir)) return jsonResponse({ error: 'INVALID_OUTBOX_SORT' }, 400);
  const query = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_operations`);
  query.searchParams.set('select', 'id,operation_type,entity_type,entity_id,status,phase,priority,total_units,completed_units,failed_units,progress_json,result_json,error_json,requires_user_action,change_seq,created_at_utc,updated_at_utc,run_after_utc,parent_operation_id');
  const snapshotAt = new Date().toISOString();
  let invoiceQueue;
  try {
    invoiceQueue = normaliseInvoiceOutboxQueueState(queueState, snapshotAt);
  } catch (error) {
    return jsonResponse({ error: String(error?.code || error?.message || 'INVALID_OUTBOX_QUEUE_STATE') }, 400);
  }
  if (status && /^[A-Z_]+$/.test(status)) query.searchParams.set('status', `eq.${status}`);
  const operationType = String(url.searchParams.get('operation_type') || '').trim().toUpperCase();
  if (operationType && /^[A-Z_]+$/.test(operationType)) query.searchParams.set('operation_type', `eq.${operationType}`);
  const entityId = String(url.searchParams.get('entity_id') || '').trim().toLowerCase();
  if (entityId) {
    if (!UUID_PATTERN.test(entityId)) return jsonResponse({ error: 'INVALID_ENTITY_ID' }, 400);
    query.searchParams.set('entity_id', `eq.${entityId}`);
  }
  const requiresAction = url.searchParams.get('requires_user_action');
  if (!channel) {
    try {
      return await handleUnifiedOutboxCursorList(env, {
        limit,
        offset,
        status,
        queueState,
        search,
        sortBy,
        sortDir,
        operationType,
        entityId,
        requiresAction,
        cursorToken: url.searchParams.get('cursor')
      });
    } catch (error) {
      const code = String(error?.code || error?.message || 'UNIFIED_OUTBOX_LIST_FAILED');
      const statusCode = code.startsWith('OUTBOX_CURSOR_') || code.startsWith('INVALID_') ? 400 : 502;
      return jsonResponse({ error: code }, statusCode);
    }
  }
  const invoiceRequiresAction = invoiceQueue.requiresAction ?? requiresAction;
  if (invoiceRequiresAction === 'true' || invoiceRequiresAction === 'false') query.searchParams.set('requires_user_action', `eq.${invoiceRequiresAction}`);
  if (invoiceQueue.expression) applyPostgrestAndExpressions(query, [invoiceQueue.expression]);
  if (search) {
    if (UUID_PATTERN.test(search)) query.searchParams.set('or', `(id.eq.${search},entity_id.eq.${search})`);
    else if (/^[a-z0-9 _-]{1,80}$/i.test(search)) {
      const term = search
        .replace(/\\/g, '\\\\')
        .replace(/_/g, '\\_')
        .replace(/%/g, '\\%')
        .replace(/ +/g, '%');
      query.searchParams.set('or', `(operation_type.ilike.*${term}*,phase.ilike.*${term}*)`);
    } else return jsonResponse({ error: 'INVALID_OUTBOX_SEARCH' }, 400);
  }
  query.searchParams.set('order', 'created_at_utc.desc,id.desc');
  query.searchParams.set('limit', String(limit));
  query.searchParams.set('offset', String(offset));
  if (channel !== 'INVOICE') {
    const legacy = normaliseUnifiedOutboxPayload(await deps.rpc('outbox_unified_list', {
      p_status: status || null,
      p_channel: channel || null,
      p_search: search || null,
      p_queue_state: queueState || null,
      p_limit: limit,
      p_offset: offset,
      p_sort_by: sortBy,
      p_sort_dir: sortDir
    }));
    return jsonResponse({ ok: true, channel, total_count: Number(legacy.total_count || 0), limit, offset, returned_count: legacy.items.length, items: legacy.items });
  }
  const invoicePromise = fetch(query, { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, prefer: 'count=exact' } });
  const response = await invoicePromise;
  const invoiceRows = await response.json().catch(() => []);
  if (!response.ok || !Array.isArray(invoiceRows)) return jsonResponse({ error: 'INVOICE_OPERATION_LIST_FAILED' }, 502);
  const invoiceItems = invoiceRows.map(invoiceOperationOutboxRow);
  const invoiceTotalRaw = Number((response.headers.get('content-range') || '').split('/')[1]);
  const invoiceTotal = Number.isFinite(invoiceTotalRaw) ? invoiceTotalRaw : invoiceItems.length;
  return jsonResponse({
    ok: true,
    channel,
    total_count: invoiceTotal,
    limit,
    offset,
    returned_count: invoiceItems.length,
    items: invoiceItems,
    queue_state_semantics: invoiceQueue.semantics
  });
}
async function handleInvoiceOutboxControl(env, req, ctx, user, deps, operationId, action) {
  const body = req.method === 'POST' ? (await parseBody(req) || {}) : {};
  const controlAction = normaliseInvoiceOperationControlAction({
    operation_id: operationId,
    action,
    retry_chunk_id: body.retry_chunk_id,
    scheduled_for_utc: body.scheduled_for_utc || body.scheduledForUtc
  });
  const result = await deps.rpc('invoice_operation_control_batch', {
    p_actions: [controlAction],
    p_actor_user_id: user.id,
    p_now_utc: new Date().toISOString()
  });
  const operations = rpcValue(result);
  if (controlResultsReleasedRunnableWork(operations)) {
    await nudgeInvoiceOperations(env, operations, { ctx, rpc: deps.rpc, lanes: ['ALL'] });
  }
  return jsonResponse({ ok: true, results: operations });
}

async function recordInvoiceDocumentAccessAudit(env, req, claims, outcome) {
  try {
    const nonceBytes = new TextEncoder().encode(String(claims?.nonce || ''));
    const nonceDigest = await crypto.subtle.digest('SHA-256', nonceBytes);
    const nonceHash = Array.from(new Uint8Array(nonceDigest))
      .map(value => value.toString(16).padStart(2, '0')).join('');
    const response = await fetch(`${env.SUPABASE_URL}/rest/v1/audit_events`, {
      method: 'POST',
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        'content-type': 'application/json',
        Prefer: 'return=minimal'
      },
      body: JSON.stringify({
        object_type: 'invoice_document_versions',
        object_id_text: String(claims?.document_version_id || ''),
        action: 'INVOICE_DOCUMENT_TOKEN_ACCESS',
        before_json: null,
        after_json: {
          invoice_id: claims?.entity_id || null,
          access_purpose: claims?.purpose || null,
          nonce_hash: nonceHash || null,
          outcome: String(outcome || 'UNKNOWN').slice(0, 64)
        },
        reason: 'Secure immutable invoice-document access',
        actor_user_id: UUID_PATTERN.test(String(claims?.sub || '')) ? claims.sub : null,
        actor_display: 'Secure invoice document recipient',
        actor_role_at_time: 'document_recipient',
        ip: req.headers.get('cf-connecting-ip') || null,
        user_agent: req.headers.get('user-agent') || null,
        correlation_id: String(claims?.document_version_id || '') || null
      })
    });
    if (!response.ok) console.warn('[INVOICE_DOCUMENT_ACCESS_AUDIT_FAILED]', response.status);
  } catch (error) {
    console.warn('[INVOICE_DOCUMENT_ACCESS_AUDIT_ERROR]', String(error?.message || error));
  }
}

async function handleDocumentAccess(env, req) {
  const token = new URL(req.url).searchParams.get('token');
  const verification = await verifyInvoiceDocumentAccessToken(
    env.INVOICE_DOCUMENT_ACCESS_SECRET,
    token,
    { expectedPurpose: 'DOWNLOAD' }
  );
  if (!verification.ok) return jsonResponse({ error: verification.code }, 401);
  if (!env.R2) return jsonResponse({ error: 'INVOICE_R2_BINDING_MISSING' }, 503);
  const claims = verification.claims;
  if (
    String(claims.entity_type || '').toUpperCase() !== 'INVOICE'
    || !UUID_PATTERN.test(String(claims.entity_id || ''))
    || !UUID_PATTERN.test(String(claims.document_version_id || ''))
  ) {
    return jsonResponse({ error: 'INVOICE_DOCUMENT_TOKEN_ENTITY_INVALID' }, 401);
  }
  const serviceHeaders = {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
  };
  const versionUrl = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_document_versions`);
  versionUrl.searchParams.set('id', `eq.${claims.document_version_id}`);
  versionUrl.searchParams.set('entity_type', 'eq.INVOICE');
  versionUrl.searchParams.set('entity_id', `eq.${claims.entity_id}`);
  versionUrl.searchParams.set('purpose', 'eq.FINAL_ISSUE');
  versionUrl.searchParams.set('status', 'eq.READY');
  versionUrl.searchParams.set('select', 'id,entity_id,r2_key,sha256,size_bytes,page_count');
  versionUrl.searchParams.set('limit', '1');
  const [versionResponse, invoiceResponse] = await Promise.all([
    fetch(versionUrl, { headers: serviceHeaders }),
    fetch(
      `${env.SUPABASE_URL}/rest/v1/invoices`
        + `?id=eq.${encodeURIComponent(claims.entity_id)}`
        + `&issued_document_version_id=eq.${encodeURIComponent(claims.document_version_id)}`
        + '&status=in.(ISSUED,PAID)&select=id,issued_document_version_id&limit=1',
      { headers: serviceHeaders }
    )
  ]);
  const [versions, invoices] = await Promise.all([
    versionResponse.json().catch(() => []),
    invoiceResponse.json().catch(() => [])
  ]);
  const version = Array.isArray(versions) ? versions[0] : null;
  if (
    !versionResponse.ok
    || !invoiceResponse.ok
    || !version?.r2_key
    || !Array.isArray(invoices)
    || !invoices[0]
  ) {
    return jsonResponse({ error: 'INVOICE_DOCUMENT_ACCESS_REVOKED' }, 404);
  }
  const object = await env.R2.get(version.r2_key);
  if (!object) {
    await recordInvoiceDocumentAccessAudit(env, req, claims, 'DOCUMENT_NOT_FOUND');
    return jsonResponse({ error: 'DOCUMENT_NOT_FOUND' }, 404);
  }
  const metadata = object.customMetadata || {};
  if (
    !metadata.sha256 || !metadata.size_bytes || !metadata.document_version_id
    || !metadata.chunk_id || !metadata.fence_token
    || metadata.sha256 !== version.sha256
    || Number(metadata.size_bytes) !== Number(version.size_bytes)
    || Number(version.size_bytes) !== Number(object.size)
    || metadata.document_version_id !== version.id
    || !UUID_PATTERN.test(String(metadata.chunk_id))
    || !Number.isSafeInteger(Number(metadata.fence_token))
  ) {
    await recordInvoiceDocumentAccessAudit(env, req, claims, 'STORAGE_IDENTITY_MISMATCH');
    return jsonResponse({ error: 'INVOICE_DOCUMENT_STORAGE_IDENTITY_MISMATCH' }, 409);
  }
  await recordInvoiceDocumentAccessAudit(env, req, claims, 'ALLOWED');
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set('content-type', headers.get('content-type') || 'application/pdf');
  headers.set('content-disposition', `attachment; filename="${String(
    verification.claims.filename || 'invoice.pdf'
  ).replace(/["\r\n]/g, '_')}"`);
  headers.set('cache-control', 'private, no-store');
  headers.set('x-content-type-options', 'nosniff');
  return new Response(object.body, { headers });
}

export async function createReadyInvoiceDocumentLink(env, descriptor, actorUserId) {
  if (!env.INVOICE_DOCUMENT_ACCESS_SECRET || String(env.INVOICE_DOCUMENT_ACCESS_SECRET).length < 32) throw new Error('INVOICE_DOCUMENT_ACCESS_SECRET_MISSING');
  if (String(descriptor?.purpose || 'FINAL_ISSUE').toUpperCase() !== 'FINAL_ISSUE') throw new Error('INVOICE_SECURE_LINK_FINAL_ISSUE_REQUIRED');
  if (!UUID_PATTERN.test(String(descriptor?.entity_id || '')) || !UUID_PATTERN.test(String(descriptor?.document_version_id || ''))) throw new Error('INVOICE_SECURE_LINK_IDENTITY_INVALID');
  const token = await createInvoiceDocumentAccessToken(
    env.INVOICE_DOCUMENT_ACCESS_SECRET,
    {
      sub: actorUserId,
      entity_type: descriptor.entity_type,
      entity_id: descriptor.entity_id,
      document_version_id: descriptor.document_version_id,
      recipient_set_hash: descriptor.recipient_set_hash || undefined,
      purpose: 'DOWNLOAD',
      filename: descriptor.filename || 'invoice.pdf'
    },
    { ttlSeconds: Number(env.INVOICE_DOCUMENT_ACCESS_TTL_SECONDS || 300) }
  );
  const configuredBase = env.INVOICE_DOCUMENT_PUBLIC_BASE_URL
    || env.PUBLIC_APP_URL
    || env.PUBLIC_DOWNLOAD_BASE_URL
    || 'https://testmode.arthur-rai.co.uk';
  return buildInvoiceDocumentDownloadUrl(new URL(configuredBase).origin, token);
}

async function loadExactReadyApplicationDocument(env, documentVersionId) {
  if (!UUID_PATTERN.test(String(documentVersionId || ''))) {
    throw Object.assign(new Error('DOCUMENT_VERSION_ID_INVALID'), { code: 'DOCUMENT_VERSION_ID_INVALID' });
  }
  const headers = {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
  };
  const versionUrl = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_document_versions`);
  versionUrl.searchParams.set('id', `eq.${documentVersionId}`);
  versionUrl.searchParams.set('status', 'eq.READY');
  versionUrl.searchParams.set('select', 'id,entity_type,entity_id,purpose,r2_key,sha256,size_bytes,page_count,status');
  versionUrl.searchParams.set('limit', '1');
  const versionResponse = await fetch(versionUrl, { headers });
  const versions = await versionResponse.json().catch(() => []);
  const version = Array.isArray(versions) ? versions[0] : null;
  if (!versionResponse.ok || !version?.r2_key || !['DRAFT_PREVIEW','TIMESHEET','FINAL_ISSUE'].includes(String(version.purpose || '').toUpperCase())) {
    throw Object.assign(new Error('READY_DOCUMENT_VERSION_NOT_FOUND'), { code: 'READY_DOCUMENT_VERSION_NOT_FOUND' });
  }
  if (String(version.entity_type).toUpperCase() === 'INVOICE') {
    const invoiceUrl = new URL(`${env.SUPABASE_URL}/rest/v1/invoices`);
    invoiceUrl.searchParams.set('id', `eq.${version.entity_id}`);
    invoiceUrl.searchParams.set('select', 'id,status,preview_document_version_id,issued_document_version_id');
    invoiceUrl.searchParams.set('limit', '1');
    const response = await fetch(invoiceUrl, { headers });
    const rows = await response.json().catch(() => []);
    const invoice = Array.isArray(rows) ? rows[0] : null;
    const purpose = String(version.purpose).toUpperCase();
    const valid = response.ok && invoice && (
      (purpose === 'DRAFT_PREVIEW'
        && String(invoice.preview_document_version_id || '') === version.id
        && String(invoice.status || '').toUpperCase() === 'DRAFT')
      || (purpose === 'FINAL_ISSUE'
        && String(invoice.issued_document_version_id || '') === version.id
        && ['ISSUED','PAID'].includes(String(invoice.status || '').toUpperCase()))
    );
    if (!valid) throw Object.assign(new Error('DOCUMENT_VERSION_POINTER_MISMATCH'), { code: 'DOCUMENT_VERSION_POINTER_MISMATCH' });
  } else if (String(version.entity_type).toUpperCase() === 'TIMESHEET') {
    if (String(version.purpose).toUpperCase() !== 'TIMESHEET') throw Object.assign(new Error('DOCUMENT_VERSION_PURPOSE_MISMATCH'), { code: 'DOCUMENT_VERSION_PURPOSE_MISMATCH' });
    const timesheetUrl = new URL(`${env.SUPABASE_URL}/rest/v1/timesheets`);
    timesheetUrl.searchParams.set('timesheet_id', `eq.${version.entity_id}`);
    timesheetUrl.searchParams.set('is_current', 'eq.true');
    timesheetUrl.searchParams.set('current_document_version_id', `eq.${version.id}`);
    timesheetUrl.searchParams.set('select', 'timesheet_id,current_document_version_id');
    timesheetUrl.searchParams.set('limit', '1');
    const response = await fetch(timesheetUrl, { headers });
    const rows = await response.json().catch(() => []);
    if (!response.ok || !Array.isArray(rows) || !rows[0]) {
      throw Object.assign(new Error('DOCUMENT_VERSION_POINTER_MISMATCH'), { code: 'DOCUMENT_VERSION_POINTER_MISMATCH' });
    }
  } else {
    throw Object.assign(new Error('DOCUMENT_VERSION_ENTITY_TYPE_INVALID'), { code: 'DOCUMENT_VERSION_ENTITY_TYPE_INVALID' });
  }
  return version;
}

async function handleReadyInvoiceDocumentPresign(env, user, documentVersionId) {
  const version = await loadExactReadyApplicationDocument(env, documentVersionId);
  const token = await createInvoiceDocumentAccessToken(
    env.INVOICE_DOCUMENT_ACCESS_SECRET,
    {
      sub: user.id,
      entity_type: version.entity_type,
      entity_id: version.entity_id,
      document_version_id: version.id,
      purpose: 'APPLICATION_DOWNLOAD',
      filename: `${String(version.purpose || 'document').toLowerCase()}.pdf`
    },
    { ttlSeconds: 300 }
  );
  const configuredBase = env.INVOICE_DOCUMENT_PUBLIC_BASE_URL
    || env.PUBLIC_APP_URL
    || 'https://test-cloudtms-backend.kier-88a.workers.dev';
  return jsonResponse({
    ok: true,
    document_version_id: version.id,
    purpose: version.purpose,
    expires_in_seconds: 300,
    url: `${new URL(configuredBase).origin}/api/invoice-document-versions/${version.id}/download?token=${encodeURIComponent(token)}`
  });
}

async function handleReadyInvoiceDocumentDownload(env, req, user, documentVersionId) {
  const token = new URL(req.url).searchParams.get('token');
  const verified = await verifyInvoiceDocumentAccessToken(
    env.INVOICE_DOCUMENT_ACCESS_SECRET,
    token,
    { expectedPurpose: 'APPLICATION_DOWNLOAD' }
  );
  if (!verified.ok || verified.claims.sub !== user.id || verified.claims.document_version_id !== documentVersionId) {
    return jsonResponse({ error: verified.code || 'DOCUMENT_ACCESS_TOKEN_INVALID' }, 401);
  }
  const version = await loadExactReadyApplicationDocument(env, documentVersionId);
  const object = await env.R2.get(version.r2_key);
  if (!object) return jsonResponse({ error: 'DOCUMENT_NOT_FOUND' }, 404);
  const metadata = object.customMetadata || {};
  if (
    metadata.sha256 !== version.sha256
    || Number(metadata.size_bytes) !== Number(version.size_bytes)
    || Number(object.size) !== Number(version.size_bytes)
    || metadata.document_version_id !== version.id
    || !UUID_PATTERN.test(String(metadata.chunk_id || ''))
    || !Number.isSafeInteger(Number(metadata.fence_token))
  ) return jsonResponse({ error: 'INVOICE_DOCUMENT_STORAGE_IDENTITY_MISMATCH' }, 409);
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set('content-type', 'application/pdf');
  headers.set('content-disposition', `inline; filename="${String(version.purpose || 'document').toLowerCase()}.pdf"`);
  headers.set('cache-control', 'private, no-store');
  headers.set('x-content-type-options', 'nosniff');
  return new Response(object.body, { headers });
}

async function handleInvoiceAsyncCapabilities(env, req, deps) {
  const user = await requireActor(env, req, deps, false);
  if (!user) return jsonResponse({ error: 'UNAUTHENTICATED' }, 401);
  const parsed = parseInvoiceAsyncAllowedUserIds(env.INVOICE_ASYNC_ALLOWED_USER_IDS);
  const pipelineEnabled = isInvoiceAsyncPipelineEnabled(env);
  const processorEnabled = String(env.INVOICE_DOCUMENT_PROCESSOR_ENABLED || '').toLowerCase() === 'true';
  const cohort = isInvoiceAsyncUserAllowed(env, {
    ...user,
    active: user.is_active ?? user.active,
    roles: [user.role, user.user_role, user.user_type]
  });
  const contractVersion = 'INVOICE_ASYNC_BACKEND_V7';
  const featureFlags = {
    batch_candidate_paging_v1: true,
    batch_selection_rules_v1: true,
    batch_result_paging_v1: true,
    generate_and_view_v1: true,
    exact_document_version_access_v1: true,
    separate_issue_delivery_state_v1: true
  };
  return jsonResponse({
    contract_version: contractVersion,
    backend_contract_version: contractVersion,
    pipeline_enabled: pipelineEnabled,
    processor_enabled: processorEnabled,
    enabled_for_user: pipelineEnabled && processorEnabled && cohort.allowed === true,
    enabled: pipelineEnabled && processorEnabled && cohort.allowed === true,
    controlled_cohort: parsed.ok && parsed.ids.length > 0,
    scheduled_enabled: String(env.INVOICE_ASYNC_SCHEDULED_ENABLED || '').toLowerCase() === 'true',
    supported_media_types: ['application/pdf','image/jpeg','image/png'],
    document_view_contract_version: 'INVOICE_DOCUMENT_VERSION_ACCESS_V1',
    heartbeat_supported: true,
    feature_flags: featureFlags,
    ...featureFlags
  }, 200, { 'x-invoice-async-contract-version': contractVersion });
}

function match(pathname, pattern) {
  const actual = pathname.split('/').filter(Boolean);
  const expected = pattern.split('/').filter(Boolean);
  if (actual.length !== expected.length) return null;
  const params = {};
  for (let index = 0; index < expected.length; index += 1) {
    if (expected[index].startsWith(':')) params[expected[index].slice(1)] = decodeURIComponent(actual[index]);
    else if (actual[index] !== expected[index]) return null;
  }
  return params;
}

function invoiceErrorStatus(error) {
  const code = String(error?.code || error?.message || error || '').toUpperCase();
  if (
    code.startsWith('BATCH_CURSOR_')
    || code.startsWith('BATCH_SELECTION_')
    || code.startsWith('BATCH_QUERY_')
    || code === 'INVOICE_BATCH_QUERY_INVALID'
    || code === 'INVOICE_BATCH_QUERY_CONTRACT_INVALID'
    || code === 'INVOICE_BATCH_QUERY_MODE_INVALID'
    || code === 'INVOICE_BATCH_DISPLAY_MODE_INVALID'
    || code === 'INVOICE_BATCH_GROUP_PRESET_INVALID'
    || code === 'INVOICE_BATCH_SORT_KEY_INVALID'
    || code === 'INVOICE_BATCH_SORT_DIRECTION_INVALID'
    || code === 'BATCH_FILTER_FIELD_UNSUPPORTED'
    || code === 'BATCH_COMMAND_TOKEN_INVALID'
    || code === 'DELIVERY_REQUEST_TOKEN_INVALID'
  ) return 400;
  if (/INVALID|MALFORMED|REQUIRED|UUID|JSON/.test(code)) return 400;
  if (/UNAUTHENTICATED|SESSION/.test(code)) return 401;
  if (/FORBIDDEN|ADMIN_REQUIRED|PERMISSION/.test(code)) return 403;
  if (/CONFLICT|SOURCE_CHANGED|CURRENT_STATE_CHANGED|STALE|ACTIVE_OPERATION|NOT_READY|BLOCKED|TERMINAL|DOCUMENT_REVISION_CHANGED|OWNERSHIP_OR_DOCUMENT_MISMATCH/.test(code)) return 409;
  if (/BACKPRESSURE|RATE_LIMIT/.test(code)) return 429;
  if (/UNAVAILABLE|BINDING_MISSING|CONFIGURATION|SECRET_MISSING|SECRET_INVALID/.test(code)) return 503;
  return 500;
}

function isInvoiceAsyncRoute(req, url) {
  const path = url.pathname;
  const method = req.method;
  if (method === 'GET' && path === '/api/invoice-documents/access') return true;
  if (method === 'GET' && path === '/api/invoice-async/capabilities') return true;
  if (method === 'GET' && [
    '/api/invoices/batch-generate/candidates',
    '/api/invoices/batch-issue/candidates',
    '/api/nhsp/invoices/candidates'
  ].includes(path)) return true;
  if (method === 'POST' && [
    '/api/invoices/batch-generate/confirm',
    '/api/invoices/batch-issue/confirm',
    '/api/invoice-operations/get',
    '/api/invoice-operations/control',
    '/api/invoices',
    '/api/invoices/tsfin/by-week',
    '/api/invoices/create-expenses',
    '/api/nhsp/invoices/run'
  ].includes(path)) return true;
  if (method === 'GET' && path === '/api/outbox') {
    const channel = String(url.searchParams.get('channel') || '').trim().toUpperCase();
    return !channel || channel === 'INVOICE';
  }
  if (method === 'GET' && match(path, '/api/invoice-operations/:operation_id')) return true;
  if (
    method === 'POST'
    && match(path, '/api/invoice-document-versions/:document_version_id/presign')
  ) return true;
  if (
    method === 'GET'
    && match(path, '/api/invoice-document-versions/:document_version_id/download')
  ) return true;
  const outbox = match(path, '/api/outbox/:channel/:operation_id');
  if (outbox && String(outbox.channel).toUpperCase() === 'INVOICE' && ['GET', 'DELETE'].includes(method)) return true;
  const retry = match(path, '/api/outbox/:channel/:operation_id/retry');
  if (retry && String(retry.channel).toUpperCase() === 'INVOICE' && method === 'POST') return true;
  const reschedule = match(path, '/api/outbox/:channel/:operation_id/reschedule');
  if (reschedule && String(reschedule.channel).toUpperCase() === 'INVOICE' && method === 'POST') return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/render')) return true;
  if (['GET', 'POST'].includes(method) && match(path, '/api/timesheets/:timesheet_id/pdf')) return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/issue')) return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/email')) return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/credit-note')) return true;
  if (method === 'GET' && match(path, '/api/invoices/:invoice_id')) return true;
  return false;
}

export async function handleInvoiceAsyncHttpRequest(req, env, ctx, deps) {
  const url = new URL(req.url);
  const path = url.pathname;
  if (req.method === 'GET' && path === '/api/invoice-documents/access') {
    return handleDocumentAccess(env, req);
  }
  if (req.method === 'GET' && path === '/api/invoice-async/capabilities') {
    return handleInvoiceAsyncCapabilities(env, req, deps);
  }
  if (!isInvoiceAsyncPipelineEnabled(env)) return null;
  if (!isInvoiceAsyncRoute(req, url)) return null;

  const user = await requireActor(env, req, deps, false);
  if (!user) return jsonResponse({ error: 'UNAUTHENTICATED' }, 401);
  const cohort = isInvoiceAsyncUserAllowed(env, { ...user, active: user.is_active ?? user.active, roles: [user.role, user.user_role, user.user_type] });
  if (!cohort.allowed) {
    if (cohort.code === 'INVOICE_ASYNC_USER_OUTSIDE_COHORT') return null;
    if (cohort.code === 'INVOICE_ASYNC_ALLOWLIST_INVALID') return jsonResponse({ error: cohort.code }, 503);
    return jsonResponse({ error: cohort.code }, 403);
  }

  try {
    if (req.method === 'GET' && path === '/api/invoices/batch-generate/candidates') {
      return handleCandidates(env, req, deps, 'invoice_batch_generate_candidates');
    }
    if (req.method === 'GET' && path === '/api/invoices/batch-issue/candidates') {
      return handleCandidates(env, req, deps, 'invoice_batch_issue_candidates');
    }
    if (req.method === 'GET' && path === '/api/nhsp/invoices/candidates') {
      return handleNhspCandidates(req, deps);
    }
    if (req.method === 'POST' && path === '/api/invoices/batch-generate/confirm') {
      return handleBatchGenerateConfirm(env, req, ctx, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoices/batch-issue/confirm') {
      return handleBatchIssueConfirm(env, req, ctx, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoice-operations/get') {
      return handleOperationGet(env, req, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoice-operations/control') {
      return handleOperationControl(env, req, ctx, user, deps);
    }
    if (req.method === 'GET' && path === '/api/outbox') {
      const channel = String(url.searchParams.get('channel') || '').trim().toUpperCase();
      if (!channel || channel === 'INVOICE') {
        return handleInvoiceOutboxList(env, req, deps);
      }
    }

    let params = match(path, '/api/invoice-operations/:operation_id');
    if (params && req.method === 'GET') {
      return handleOperationGet(env, req, user, deps, params.operation_id);
    }
    params = match(path, '/api/invoice-document-versions/:document_version_id/presign');
    if (params && req.method === 'POST') {
      return handleReadyInvoiceDocumentPresign(
        env,
        user,
        params.document_version_id
      );
    }
    params = match(path, '/api/invoice-document-versions/:document_version_id/download');
    if (params && req.method === 'GET') {
      return handleReadyInvoiceDocumentDownload(
        env,
        req,
        user,
        params.document_version_id
      );
    }
    params = match(path, '/api/outbox/:channel/:operation_id');
    if (params && String(params.channel).toUpperCase() === 'INVOICE') {
      if (req.method === 'GET') {
        return handleOperationGet(env, req, user, deps, params.operation_id);
      }
      if (req.method === 'DELETE') {
        return handleInvoiceOutboxControl(
          env, req, ctx, user, deps, params.operation_id, 'CANCEL'
        );
      }
    }
    params = match(path, '/api/outbox/:channel/:operation_id/retry');
    if (
      params
      && req.method === 'POST'
      && String(params.channel).toUpperCase() === 'INVOICE'
    ) {
      return handleInvoiceOutboxControl(
        env, req, ctx, user, deps, params.operation_id, 'RETRY'
      );
    }
    params = match(path, '/api/outbox/:channel/:operation_id/reschedule');
    if (
      params
      && req.method === 'POST'
      && String(params.channel).toUpperCase() === 'INVOICE'
    ) {
      return handleInvoiceOutboxControl(
        env, req, ctx, user, deps, params.operation_id, 'RESCHEDULE'
      );
    }
    params = match(path, '/api/invoices/:invoice_id/render');
    if (params && req.method === 'POST') {
      return handleViewDocument(env, req, ctx, user, deps, 'INVOICE', params.invoice_id);
    }
    params = match(path, '/api/timesheets/:timesheet_id/pdf');
    if (params && ['GET', 'POST'].includes(req.method)) {
      return handleViewDocument(env, req, ctx, user, deps, 'TIMESHEET', params.timesheet_id);
    }
    params = match(path, '/api/invoices/:invoice_id/issue');
    if (params && req.method === 'POST') {
      return handleIssueOne(env, req, ctx, user, deps, params.invoice_id);
    }
    params = match(path, '/api/invoices/:invoice_id/email');
    if (params && req.method === 'POST') {
      return handleDeliverOne(env, req, ctx, user, deps, params.invoice_id);
    }
    params = match(path, '/api/invoices/:invoice_id/credit-note');
    if (params && req.method === 'POST') {
      const body = await parseBody(req) || {};
      return startCommands(env, req, ctx, user, [{
        command_type: 'GENERATE_CREDIT_NOTE',
        base_invoice_id: params.invoice_id,
        credit_reason: body.credit_reason || body.reason,
        command_token: commandToken(req, body)
      }], deps, ['DATABASE']);
    }
    params = match(path, '/api/invoices/:invoice_id');
    if (params && req.method === 'GET') {
      const detail = await deps.rpc('invoice_detail_get', {
        p_invoice_id: params.invoice_id,
        p_actor_user_id: user.id
      });
      return jsonResponse(rpcValue(detail));
    }

    if (req.method === 'POST' && path === '/api/invoices') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      return startCommands(env, req, ctx, user, [
        generationCommandFromBody(req, body, 'GENERATE_SELECTED')
      ], deps, ['DATABASE']);
    }
    if (req.method === 'POST' && path === '/api/invoices/tsfin/by-week') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      return startCommands(env, req, ctx, user, [
        generationCommandFromBody(req, body, 'GENERATE_BY_WEEK')
      ], deps, ['DATABASE']);
    }
    if (req.method === 'POST' && path === '/api/invoices/create-expenses') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      return startCommands(env, req, ctx, user, [
        generationCommandFromBody(req, body, 'GENERATE_EXPENSES')
      ], deps, ['DATABASE']);
    }
    if (req.method === 'POST' && path === '/api/nhsp/invoices/run') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      const sourceIds = body.nhsp_shift_ids || body.source_ids || body.timesheet_ids;
      return startCommands(env, req, ctx, user, [{
        ...generationCommandFromBody(req, { ...body, source_ids: sourceIds }, 'GENERATE_NHSP'),
        nhsp_shift_ids: body.nhsp_shift_ids
          ? canonicalUuidArray(body.nhsp_shift_ids)
          : undefined
      }], deps, ['DATABASE']);
    }
    return null;
  } catch (error) {
    const code = String(error?.code || error?.message || error || 'INVOICE_PIPELINE_UNEXPECTED').slice(0, 160);
    return jsonResponse({ error: code }, invoiceErrorStatus(error));
  }
}

export const invoiceAsyncHttpInternals = Object.freeze({
  rpcValue,
  canonicalUuidArray,
  canonicalEmailArray,
  boolValue,
  generationCommandFromBody,
  normaliseInvoiceBatchFilters,
  normaliseInvoiceBatchSort,
  normaliseInvoiceBatchSelectionRules,
  hashInvoiceBatchQuery,
  encodeInvoiceBatchCursor,
  decodeInvoiceBatchCursor,
  candidateGroupsFromRpc,
  encodeUnifiedOutboxCursor,
  decodeUnifiedOutboxCursor,
  compareCursorOutboxRows,
  legacyQueueState,
  match
});
