import {
  buildInvoiceDocumentDownloadUrl,
  createInvoiceDocumentAccessToken,
  verifyInvoiceDocumentAccessToken
} from './invoice-document-access.js';
import {
  checkInvoiceDocumentProcessorReady,
  isInvoiceAsyncPipelineEnabled,
  nudgeInvoiceOperations,
  validateQueueRuntimeConfiguration
} from './invoice-queue-runtime.js';
import {
  isInvoiceAsyncUserAllowed,
  parseInvoiceAsyncAccessMode,
  parseInvoiceAsyncAllowedUserIds
} from './invoice-queue-security.js';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const INVOICE_ASYNC_CONTRACT_VERSION = 'INVOICE_ASYNC_BACKEND_V8';
const INVOICE_ASYNC_DB_CONTRACT_VERSION = 'INVOICE_ASYNC_DB_V2';
const INVOICE_BATCH_QUERY_CONTRACT = 'INVOICE_BATCH_QUERY_V2';
const INVOICE_BATCH_CANDIDATE_CONTRACT = 'INVOICE_BATCH_CANDIDATES_V2';
const INVOICE_BATCH_SELECTION_CONTRACT = 'INVOICE_BATCH_SELECTION_V2';
const INVOICE_BATCH_SELECTION_ROOT_CONTRACT = 'INVOICE_BATCH_SELECTION_ROOT_V2';
const INVOICE_BATCH_CURSOR_CONTRACT = 'INVOICE_BATCH_CURSOR_V2';
const INVOICE_BATCH_RESULT_CURSOR_CONTRACT = 'INVOICE_BATCH_RESULT_CURSOR_V2';
const INVOICE_BATCH_PROGRESS_CONTRACT = 'INVOICE_BATCH_PROGRESS_V2';
const INVOICE_VIEWER_CONTRACT = 'INVOICE_VIEWER_V2';
const INVOICE_DOCUMENT_ACCESS_CONTRACT = 'INVOICE_DOCUMENT_VERSION_ACCESS_V1';
const INVOICE_BATCH_REQUEST_MAX_BYTES = 4_194_304;
const INVOICE_BATCH_SELECTION_MAX_BYTES = 3_145_728;
const INVOICE_BATCH_CURSOR_MAX_BYTES = 8_192;
const INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS = 1_800;
const INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS = 86_400;
const INVOICE_BATCH_DATABASE_CONTRACT_CACHE_MS = 30_000;
const invoiceAsyncDatabaseContractCache = new Map();
const invoiceAsyncProcessorReadyCache = new WeakMap();
const INVOICE_BATCH_MANDATORY_FEATURES = Object.freeze([
  'batch_candidate_paging_v2',
  'batch_selection_rules_v2',
  'batch_selection_summary_v2',
  'batch_facets_v2',
  'batch_result_paging_v2',
  'generate_and_view_v2',
  'exact_document_version_access_v1',
  'separate_issue_delivery_state_v2',
  'bounded_viewer_contract_v2',
  'heartbeat_supported'
]);
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

function commandToken(req, body = {}, options = {}) {
  const bodyFields = Array.isArray(options.bodyFields)
    ? options.bodyFields
    : ['command_token'];
  const suppliedValues = [
    ...bodyFields.map(field => body?.[field]),
    req?.headers?.get?.('idempotency-key'),
    req?.headers?.get?.('x-idempotency-key')
  ]
    .filter(value => value != null)
    .map(value => String(value).trim())
    .filter(Boolean);
  const distinctValues = [...new Set(suppliedValues)];
  if (distinctValues.length > 1) {
    throw invoiceBatchContractError(
      options.invalidCode || 'BATCH_COMMAND_TOKEN_INVALID'
    );
  }
  const fallbackValue = options.internal === true
    ? String(options.generatedToken ?? '').trim()
    : '';
  const text = distinctValues[0] || fallbackValue;
  if (!text) throw invoiceBatchContractError(options.requiredCode || 'BATCH_COMMAND_TOKEN_REQUIRED');
  if (text.length > 256 || /[\u0000-\u001f\u007f]/.test(text)) {
    throw invoiceBatchContractError(options.invalidCode || 'BATCH_COMMAND_TOKEN_INVALID');
  }
  return text;
}

function normaliseDeliveryRequestToken(value, commandTokenValue, options = {}) {
  const text = String(value ?? '').trim();
  if (!text) {
    if (options.required === false) return null;
    throw invoiceBatchContractError('DELIVERY_REQUEST_TOKEN_REQUIRED');
  }
  if (text.length > 256 || /[\u0000-\u001f\u007f]/.test(text)
      || text === commandTokenValue) {
    throw invoiceBatchContractError('DELIVERY_REQUEST_TOKEN_INVALID');
  }
  return text;
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
    command_token: commandToken(req, body, {
      requiredCode: 'GENERATE_COMMAND_TOKEN_REQUIRED',
      invalidCode: 'GENERATE_COMMAND_TOKEN_INVALID'
    })
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
  'status_codes', 'blocker_codes', 'search', 'allow_early', 'display_mode', 'invoice_streams'
]);
const INVOICE_BATCH_SORT_KEYS = Object.freeze(['group_preset', 'sort_key', 'sort_direction']);
const INVOICE_BATCH_QUERY_MODE_FIELDS = Object.freeze({
  PAGE: Object.freeze([
    'contract_version', 'action', 'mode', 'snapshot', 'page_size', 'cursor',
    'filters', 'sort', 'selection'
  ]),
  FACETS: Object.freeze([
    'contract_version', 'action', 'mode', 'snapshot', 'filters', 'sort',
    'selection', 'facet_request'
  ]),
  SUMMARY: Object.freeze([
    'contract_version', 'action', 'mode', 'snapshot', 'filters', 'sort',
    'selection', 'group_selectors'
  ]),
  EXPLICIT_KEYS: Object.freeze([
    'contract_version', 'action', 'mode', 'snapshot', 'filters', 'sort',
    'selection', 'selection_keys', 'expected_source_revisions'
  ])
});
const INVOICE_BATCH_GROUP_PRESETS = Object.freeze([
  'WEEK_CLIENT_CANDIDATE', 'CLIENT_WEEK_CANDIDATE', 'CANDIDATE_WEEK_CLIENT', 'STATUS_WEEK_CLIENT'
]);
const INVOICE_BATCH_GENERATE_SORT_KEYS = Object.freeze([
  'WEEK_ENDING_DATE', 'CLIENT_NAME', 'CANDIDATE_NAME', 'TOTAL_EX_VAT', 'TOTAL_INC_VAT', 'STATUS'
]);
const INVOICE_BATCH_ISSUE_SORT_KEYS = Object.freeze([...INVOICE_BATCH_GENERATE_SORT_KEYS, 'INVOICE_NUMBER']);
const INVOICE_BATCH_GENERATE_STATUS_CODES = Object.freeze(['READY', 'BLOCKED', 'IN_PROGRESS', 'STALE', 'FAILED']);
const INVOICE_BATCH_ISSUE_STATUS_CODES = Object.freeze([
  'READY', 'READY_SEND_BLOCKED', 'BLOCKED', 'IN_PROGRESS', 'STALE', 'FAILED'
]);
const INVOICE_OPERATION_RESULT_CATEGORIES = Object.freeze([
  'ALL', 'READY', 'COMPLETED', 'IN_PROGRESS', 'GENERATED', 'REGENERATED',
  'ISSUED', 'ISSUED_SEND_BLOCKED', 'ALREADY_ACTIVE', 'BLOCKED', 'FAILED', 'CHANGED'
]);

function invoiceBatchContractError(code) {
  return Object.assign(new Error(code), { code });
}

function plainInvoiceBatchObject(value, code = 'BATCH_QUERY_INVALID') {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw invoiceBatchContractError(code);
  }
  return value;
}

function validateInvoiceBatchSourceFields(source, mode) {
  const request = plainInvoiceBatchObject(source);
  const normalizedMode = String(mode || request.mode || '').trim().toUpperCase();
  const allowed = INVOICE_BATCH_QUERY_MODE_FIELDS[normalizedMode];
  if (!allowed) throw invoiceBatchContractError('INVOICE_BATCH_QUERY_MODE_INVALID');
  if (Object.keys(request).some(key => !allowed.includes(key))) {
    throw invoiceBatchContractError('INVOICE_BATCH_QUERY_MODE_FIELD_INVALID');
  }
  return normalizedMode;
}

function invoiceBatchSourceValues(source, key) {
  const value = source?.[key];
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw invoiceBatchContractError('INVOICE_BATCH_FILTER_INVALID');
  return value;
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

function normaliseInvoiceBatchFilters(source, action = 'GENERATE', options = {}) {
  const request = plainInvoiceBatchObject(source);
  const filters = request.filters === undefined ? request : plainInvoiceBatchObject(
    request.filters,
    'INVOICE_BATCH_FILTER_INVALID'
  );
  if (Object.keys(filters).some(key => !INVOICE_BATCH_FILTER_KEYS.includes(key))) {
    throw invoiceBatchContractError('INVOICE_BATCH_FILTER_UNKNOWN_FIELD');
  }
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
  const statusCodes = normaliseInvoiceBatchArray(invoiceBatchSourceValues(filters, 'status_codes'), codeOptions);
  const allowedStatuses = normalizedAction === 'ISSUE'
    ? INVOICE_BATCH_ISSUE_STATUS_CODES
    : INVOICE_BATCH_GENERATE_STATUS_CODES;
  if (statusCodes.some(code => !allowedStatuses.includes(code))) {
    throw invoiceBatchContractError('BATCH_FILTER_STATUS_UNSUPPORTED');
  }
  const weekEndingFrom = normaliseInvoiceBatchDate(filters.week_ending_from, 'BATCH_FILTER_DATE_INVALID');
  const weekEndingTo = normaliseInvoiceBatchDate(filters.week_ending_to, 'BATCH_FILTER_DATE_INVALID');
  if (weekEndingFrom && weekEndingTo && weekEndingFrom > weekEndingTo) {
    throw invoiceBatchContractError('BATCH_FILTER_DATE_RANGE_INVALID');
  }
  const search = String(filters.search || '').trim();
  if (search.length > 200) throw invoiceBatchContractError('BATCH_FILTER_SEARCH_TOO_LONG');
  const displayMode = String(filters.display_mode || 'ALL').trim().toUpperCase();
  if (!['ALL', 'READY', 'BLOCKED'].includes(displayMode)) {
    throw invoiceBatchContractError('INVOICE_BATCH_DISPLAY_MODE_INVALID');
  }
  let invoiceStreams;
  if (options.forcedInvoiceStreams) {
    invoiceStreams = normaliseInvoiceBatchArray(options.forcedInvoiceStreams, {
      maximum: 20,
      normalise: value => String(value || '').trim().toUpperCase(),
      validate: value => /^[A-Z0-9][A-Z0-9_:-]{0,119}$/.test(value),
      errorCode: 'INVOICE_BATCH_FILTER_INVALID'
    });
  } else {
    if (filters.invoice_streams !== undefined && options.allowInvoiceStreams !== true) {
      throw invoiceBatchContractError('INVOICE_BATCH_FILTER_UNKNOWN_FIELD');
    }
    invoiceStreams = normaliseInvoiceBatchArray(invoiceBatchSourceValues(filters, 'invoice_streams'), {
      maximum: 20,
      normalise: value => String(value || '').trim().toUpperCase(),
      validate: value => /^[A-Z0-9][A-Z0-9_:-]{0,119}$/.test(value),
      errorCode: 'INVOICE_BATCH_FILTER_INVALID'
    });
  }
  return {
    client_ids: normaliseInvoiceBatchArray(invoiceBatchSourceValues(filters, 'client_ids'), uuidOptions),
    candidate_ids: normaliseInvoiceBatchArray(invoiceBatchSourceValues(filters, 'candidate_ids'), uuidOptions),
    week_endings: normaliseInvoiceBatchArray(invoiceBatchSourceValues(filters, 'week_endings'), weekOptions),
    week_ending_from: weekEndingFrom,
    week_ending_to: weekEndingTo,
    status_codes: statusCodes,
    blocker_codes: normaliseInvoiceBatchArray(invoiceBatchSourceValues(filters, 'blocker_codes'), {
      ...codeOptions,
      maximum: 250
    }),
    search: search || null,
    allow_early: normaliseInvoiceBatchBoolean(filters.allow_early, false),
    display_mode: displayMode,
    invoice_streams: invoiceStreams
  };
}

function normaliseInvoiceBatchSort(source, action = 'GENERATE') {
  const request = plainInvoiceBatchObject(source);
  const sortSource = request.sort === undefined ? request : plainInvoiceBatchObject(
    request.sort,
    'INVOICE_BATCH_SORT_INVALID'
  );
  if (Object.keys(sortSource).some(key => !INVOICE_BATCH_SORT_KEYS.includes(key))) {
    throw invoiceBatchContractError('INVOICE_BATCH_SORT_INVALID');
  }
  const normalizedAction = String(action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(normalizedAction)) throw invoiceBatchContractError('BATCH_QUERY_ACTION_MISMATCH');
  const groupPreset = String(sortSource.group_preset || 'WEEK_CLIENT_CANDIDATE').trim().toUpperCase();
  const sortKey = String(sortSource.sort_key || 'WEEK_ENDING_DATE').trim().toUpperCase();
  const sortDirection = String(sortSource.sort_direction || 'ASC').trim().toUpperCase();
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
  if (selection.contract_version !== INVOICE_BATCH_SELECTION_CONTRACT) {
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
    STATUS: ['status_code'],
    WEEK_CLIENT: ['week_ending_date', 'client_id'],
    WEEK_CLIENT_CANDIDATE: ['week_ending_date', 'client_id', 'candidate_id'],
    STATUS_WEEK: ['status_code', 'week_ending_date'],
    STATUS_WEEK_CLIENT: ['status_code', 'week_ending_date', 'client_id'],
    DIMENSION_GROUP: null
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
    if (Object.keys(selector).some(key => ![
      'type', 'selection_key', 'week_ending_date', 'client_id', 'candidate_id', 'status_code'
    ].includes(key))) {
      throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_UNKNOWN_FIELD');
    }
    const type = String(selector.type || '').trim().toUpperCase();
    let requiredFields = requiredFieldsByType[type];
    if (type === 'DIMENSION_GROUP') {
      const allowedDimensions = ['week_ending_date', 'client_id', 'candidate_id', 'status_code'];
      requiredFields = allowedDimensions.filter(key =>
        selector[key] !== undefined && selector[key] !== null && selector[key] !== ''
      );
      if (!requiredFields.length) throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
    } else if (!requiredFields) {
      throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
    }
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
      } else if (key === 'status_code') {
        const value = String(selector[key] || '').trim().toUpperCase();
        if (!/^[A-Z0-9][A-Z0-9_:-]{0,119}$/.test(value)) {
          throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
        }
        normalizedSelector[key] = value;
      } else {
        const value = String(selector[key] || '').trim().toLowerCase();
        if (!UUID_PATTERN.test(value)) throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
        normalizedSelector[key] = value;
      }
    }
    return { sequence, action, selector: normalizedSelector };
  });
  const normalized = {
    contract_version: INVOICE_BATCH_SELECTION_CONTRACT,
    mode: 'IMPLICIT_ALL',
    default_selected: true,
    rules
  };
  if (new TextEncoder().encode(postgresJsonbTextForInvoiceBatch(normalized)).byteLength
      > INVOICE_BATCH_SELECTION_MAX_BYTES) {
    throw invoiceBatchContractError('BATCH_SELECTION_PAYLOAD_TOO_LARGE');
  }
  return normalized;
}

function normaliseInvoiceBatchSnapshot(value, action, options = {}) {
  if (value === null && options.allowNull === true) return null;
  const snapshot = plainInvoiceBatchObject(value, 'BATCH_SNAPSHOT_INVALID');
  const allowed = new Set([
    'contract_version', 'action', 'at_utc', 'revision',
    'expires_at_utc', 'key_id', 'token'
  ]);
  if (Object.keys(snapshot).some(key => !allowed.has(key))) {
    throw invoiceBatchContractError('BATCH_SNAPSHOT_INVALID');
  }
  const normalizedAction = String(action || '').trim().toUpperCase();
  if (snapshot.contract_version !== 'INVOICE_BATCH_SNAPSHOT_V2'
      || String(snapshot.action || '').trim().toUpperCase() !== normalizedAction) {
    throw invoiceBatchContractError('BATCH_SNAPSHOT_INVALID');
  }
  const strictTimestamp = (raw, code) => {
    const text = String(raw || '').trim();
    const parsed = new Date(text);
    if (!text || !Number.isFinite(parsed.getTime()) || parsed.toISOString() !== text) {
      throw invoiceBatchContractError(code);
    }
    return text;
  };
  const atUtc = strictTimestamp(snapshot.at_utc, 'BATCH_SNAPSHOT_INVALID');
  const expiresAtUtc = strictTimestamp(snapshot.expires_at_utc, 'BATCH_SNAPSHOT_INVALID');
  if (expiresAtUtc <= atUtc) throw invoiceBatchContractError('BATCH_SNAPSHOT_INVALID');
  const revision = String(snapshot.revision || '').trim();
  if (!/^[0-9]{1,20}$/.test(revision)) throw invoiceBatchContractError('BATCH_SNAPSHOT_INVALID');
  const keyId = String(snapshot.key_id || '').trim();
  if (!/^[a-z0-9][a-z0-9._-]{0,63}$/.test(keyId)) {
    throw invoiceBatchContractError('BATCH_SNAPSHOT_INVALID');
  }
  const token = String(snapshot.token || '').trim();
  if (!token || token.length > INVOICE_BATCH_CURSOR_MAX_BYTES) {
    throw invoiceBatchContractError('BATCH_SNAPSHOT_INVALID');
  }
  const result = {
    contract_version: 'INVOICE_BATCH_SNAPSHOT_V2',
    action: normalizedAction,
    at_utc: atUtc,
    revision,
    expires_at_utc: expiresAtUtc,
    key_id: keyId,
    token
  };
  return result;
}

function normaliseInvoiceBatchGroupSelectors(value) {
  const selectors = value === undefined ? [] : value;
  if (!Array.isArray(selectors) || selectors.length > 400) {
    throw invoiceBatchContractError('INVOICE_BATCH_QUERY_MODE_FIELD_INVALID');
  }
  const normalized = selectors.map(selector => normaliseInvoiceBatchSelectionRules({
    contract_version: INVOICE_BATCH_SELECTION_CONTRACT,
    mode: 'IMPLICIT_ALL',
    default_selected: true,
    rules: [{ sequence: 1, action: 'INCLUDE', selector }]
  }).rules[0].selector);
  const identities = normalized.map(postgresJsonbTextForInvoiceBatch);
  if (new Set(identities).size !== identities.length) {
    throw invoiceBatchContractError('BATCH_SELECTION_SELECTOR_INVALID');
  }
  return normalized;
}

function normaliseInvoiceBatchFacetRequest(value) {
  const request = plainInvoiceBatchObject(value, 'BATCH_FACET_REQUEST_INVALID');
  const allowed = new Set(['kinds', 'search', 'limit_per_kind', 'cursors']);
  if (Object.keys(request).some(key => !allowed.has(key))) {
    throw invoiceBatchContractError('BATCH_FACET_REQUEST_INVALID');
  }
  const allowedKinds = new Set(['CLIENTS', 'CANDIDATES', 'WEEK_ENDINGS', 'STATUSES', 'BLOCKERS']);
  if (!Array.isArray(request.kinds) || request.kinds.length < 1 || request.kinds.length > 5) {
    throw invoiceBatchContractError('BATCH_FACET_REQUEST_INVALID');
  }
  const kinds = [...new Set(request.kinds.map(kind => String(kind || '').trim().toUpperCase()))];
  if (kinds.length !== request.kinds.length || kinds.some(kind => !allowedKinds.has(kind))) {
    throw invoiceBatchContractError('BATCH_FACET_REQUEST_INVALID');
  }
  const search = request.search == null ? null : String(request.search).trim();
  if (search && search.length > 200) throw invoiceBatchContractError('BATCH_FACET_REQUEST_INVALID');
  const limit = Number(request.limit_per_kind ?? 100);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
    throw invoiceBatchContractError('BATCH_FACET_REQUEST_INVALID');
  }
  const cursors = request.cursors === undefined
    ? {}
    : plainInvoiceBatchObject(request.cursors, 'BATCH_FACET_REQUEST_INVALID');
  const kindNames = {
    CLIENTS: 'clients',
    CANDIDATES: 'candidates',
    WEEK_ENDINGS: 'week_endings',
    STATUSES: 'statuses',
    BLOCKERS: 'blockers'
  };
  if (Object.keys(cursors).some(kind => !Object.values(kindNames).includes(kind))) {
    throw invoiceBatchContractError('BATCH_FACET_REQUEST_INVALID');
  }
  const requestedCursorNames = new Set(kinds.map(kind => kindNames[kind]));
  if (Object.keys(cursors).some(kind => !requestedCursorNames.has(kind))) {
    throw invoiceBatchContractError('BATCH_FACET_REQUEST_INVALID');
  }
  const normalizedCursors = {};
  for (const kind of kinds) {
    const name = kindNames[kind];
    const token = cursors[name];
    if (token === undefined || token === null || token === '') continue;
    if (typeof token !== 'string' || token.length > INVOICE_BATCH_CURSOR_MAX_BYTES) {
      throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    }
    normalizedCursors[name] = token;
  }
  return {
    kinds,
    search: search || null,
    limit_per_kind: limit,
    cursors: normalizedCursors
  };
}

function normaliseInvoiceBatchExplicitKeys(source, options = {}) {
  const keys = source.selection_keys;
  const revisions = source.expected_source_revisions;
  const maximum = Number(options.maximum ?? 100);
  const exactCount = options.exactCount == null
    ? null
    : Number(options.exactCount);
  if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 100
      || (exactCount != null
        && (!Number.isSafeInteger(exactCount)
          || exactCount < 1
          || exactCount > maximum))
      || !Array.isArray(keys) || keys.length < 1 || keys.length > maximum
      || (exactCount != null && keys.length !== exactCount)
      || !revisions || typeof revisions !== 'object' || Array.isArray(revisions)) {
    throw invoiceBatchContractError('BATCH_EXPLICIT_KEYS_INVALID');
  }
  const normalizedKeys = keys.map(value => String(value || '').trim());
  if (normalizedKeys.some(value => !value || value.length > 512)
      || new Set(normalizedKeys).size !== normalizedKeys.length) {
    throw invoiceBatchContractError('BATCH_EXPLICIT_KEYS_INVALID');
  }
  const normalizedRevisions = {};
  if (Object.keys(revisions).some(key => !normalizedKeys.includes(key))) {
    throw invoiceBatchContractError('BATCH_EXPLICIT_KEYS_INVALID');
  }
  for (const key of normalizedKeys) {
    const revision = String(revisions[key] || '').trim();
    if (!revision || revision.length > 512) throw invoiceBatchContractError('BATCH_EXPLICIT_KEYS_INVALID');
    normalizedRevisions[key] = revision;
  }
  return { selection_keys: normalizedKeys, expected_source_revisions: normalizedRevisions };
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

async function hashInvoiceBatchFilter(action, filters, sort) {
  const normalizedAction = String(action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(normalizedAction)) throw invoiceBatchContractError('BATCH_QUERY_ACTION_MISMATCH');
  return sha256Text(postgresJsonbTextForInvoiceBatch({
    action: normalizedAction,
    filters,
    sort
  }));
}

async function hashInvoiceBatchQuery(action, filters, sort, snapshotValue) {
  const normalizedAction = String(action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(normalizedAction)) throw invoiceBatchContractError('BATCH_QUERY_ACTION_MISMATCH');
  const snapshot = normaliseInvoiceBatchSnapshot(snapshotValue, normalizedAction);
  return sha256Text(postgresJsonbTextForInvoiceBatch({
    contract_version: INVOICE_BATCH_QUERY_CONTRACT,
    action: normalizedAction,
    filters,
    sort,
    snapshot: {
      contract_version: snapshot.contract_version,
      action: snapshot.action,
      at_utc: snapshot.at_utc,
      revision: snapshot.revision,
      expires_at_utc: snapshot.expires_at_utc,
      key_id: snapshot.key_id ?? null
    }
  }));
}

async function hashInvoiceBatchSelection(selection) {
  return sha256Text(postgresJsonbTextForInvoiceBatch(
    normaliseInvoiceBatchSelectionRules(selection)
  ));
}

function normaliseInvoiceBatchCursorValues(value, options = {}) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const kind = String(options.kind || 'PAGE').trim().toUpperCase();
  const allowedByKind = {
    PAGE: ['after_selection_key', 'after_sort_date', 'after_sort_text', 'after_sort_numeric'],
    CLIENTS: ['after_label', 'after_id'],
    CANDIDATES: ['after_label', 'after_id'],
    WEEK_ENDINGS: ['after_value'],
    STATUSES: ['after_code'],
    BLOCKERS: ['after_code']
  };
  const allowed = allowedByKind[kind];
  if (!allowed) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  if (Object.keys(value).some(key => !allowed.includes(key))) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const result = {};
  if (kind === 'PAGE' && value.after_selection_key !== undefined && value.after_selection_key !== null) {
    const text = String(value.after_selection_key).trim();
    if (!text || text.length > 512) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_selection_key = text;
  }
  if (kind === 'PAGE' && value.after_sort_date !== undefined && value.after_sort_date !== null) {
    result.after_sort_date = normaliseInvoiceBatchDate(value.after_sort_date, 'BATCH_CURSOR_INVALID');
  }
  if (kind === 'PAGE' && value.after_sort_text !== undefined && value.after_sort_text !== null) {
    const text = String(value.after_sort_text);
    if (text.length > 512) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_sort_text = text;
  }
  if (kind === 'PAGE' && value.after_sort_numeric !== undefined && value.after_sort_numeric !== null) {
    const text = String(value.after_sort_numeric).trim();
    if (!/^[+-]?\d+(?:\.\d+)?$/.test(text) || text.length > 100) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_sort_numeric = text;
  }
  if (['CLIENTS', 'CANDIDATES'].includes(kind)) {
    const label = String(value.after_label || '').trim();
    const id = String(value.after_id || '').trim().toLowerCase();
    if (!label || label.length > 512 || !UUID_PATTERN.test(id)) {
      throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    }
    result.after_label = label;
    result.after_id = id;
  } else if (kind === 'WEEK_ENDINGS') {
    result.after_value = normaliseInvoiceBatchDate(value.after_value, 'BATCH_CURSOR_INVALID');
  } else if (['STATUSES', 'BLOCKERS'].includes(kind)) {
    const code = String(value.after_code || '').trim().toUpperCase();
    if (!/^[A-Z0-9][A-Z0-9_:-]{0,119}$/.test(code)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    result.after_code = code;
  }
  if (kind === 'PAGE') {
    const sortKeys = ['after_sort_date', 'after_sort_text', 'after_sort_numeric']
      .filter(key => result[key] !== undefined);
    if (!result.after_selection_key || sortKeys.length !== 1) {
      throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    }
  }
  if (!Object.keys(result).length) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  return result;
}

function cursorSecret(env, kind) {
  const explicit = kind === 'RESULT'
    ? env?.INVOICE_BATCH_RESULT_CURSOR_SECRET
    : env?.INVOICE_BATCH_CANDIDATE_CURSOR_SECRET;
  return explicit || env?.SESSION_TOKEN_SECRET;
}

async function signInvoiceBatchCursor(secret, domain, encodedPayload) {
  if (!secret || String(secret).length < 32) throw invoiceBatchContractError('BATCH_CURSOR_SECRET_INVALID');
  const key = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(String(secret)),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign(
    'HMAC', key, new TextEncoder().encode(`${domain}.${encodedPayload}`)
  );
  return encodeOutboxCursorPart(new Uint8Array(signature));
}

async function encodeInvoiceBatchCursor(env, input) {
  const action = String(input?.action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(action)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const snapshot = normaliseInvoiceBatchSnapshot(input?.snapshot, action);
  const filterHash = String(input?.filter_hash || '').trim().toLowerCase();
  const queryHash = String(input?.query_hash || '').trim().toLowerCase();
  if (!SHA256_PATTERN.test(filterHash) || !SHA256_PATTERN.test(queryHash)) {
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  const sort = normaliseInvoiceBatchSort({ sort: input?.sort || {} }, action);
  const cursorKind = String(input?.cursor_kind || 'PAGE').trim().toUpperCase();
  const keyset = normaliseInvoiceBatchCursorValues(
    input?.next_cursor_values || input?.keyset || {},
    { kind: cursorKind }
  );
  const now = new Date(input?.issued_at_utc || Date.now());
  if (!Number.isFinite(now.getTime())) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const configuredTtl = Number(env?.INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS);
  const ttlSeconds = Number.isSafeInteger(configuredTtl) && configuredTtl > 0
    ? Math.min(configuredTtl, INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS)
    : INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS;
  const expiry = new Date(Math.min(
    now.getTime() + ttlSeconds * 1000,
    new Date(snapshot.expires_at_utc).getTime()
  ));
  const payload = {
    version: 2,
    contract_version: INVOICE_BATCH_CURSOR_CONTRACT,
    cursor_kind: cursorKind,
    action,
    snapshot,
    query_hash: queryHash,
    filter_hash: filterHash,
    sort,
    keyset,
    issued_at_utc: now.toISOString(),
    expires_at_utc: expiry.toISOString()
  };
  const encodedPayload = encodeOutboxCursorPart(new TextEncoder().encode(
    postgresJsonbTextForInvoiceBatch(payload)
  ));
  const signature = await signInvoiceBatchCursor(
    cursorSecret(env, 'CANDIDATE'),
    'invoice-batch-candidate-cursor-v2',
    encodedPayload
  );
  const token = `${encodedPayload}.${signature}`;
  if (token.length > INVOICE_BATCH_CURSOR_MAX_BYTES) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  return token;
}

async function decodeInvoiceBatchCursor(env, token, expected = {}) {
  const tokenText = String(token || '');
  if (tokenText.length > INVOICE_BATCH_CURSOR_MAX_BYTES) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  const parts = tokenText.split('.');
  if (parts.length !== 2 || parts.some(part => !part)) throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  let expectedSignature;
  let actualSignature;
  try {
    expectedSignature = decodeCanonicalInvoiceCursorPart(await signInvoiceBatchCursor(
      cursorSecret(env, 'CANDIDATE'),
      'invoice-batch-candidate-cursor-v2',
      parts[0]
    ), 'BATCH_CURSOR_INVALID');
    actualSignature = decodeCanonicalInvoiceCursorPart(parts[1], 'BATCH_CURSOR_INVALID');
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
    payload = JSON.parse(new TextDecoder().decode(
      decodeCanonicalInvoiceCursorPart(parts[0], 'BATCH_CURSOR_INVALID')
    ));
  } catch {
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  if (payload?.version !== 2 || payload?.contract_version !== INVOICE_BATCH_CURSOR_CONTRACT) {
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  const cursorKind = String(payload.cursor_kind || '').trim().toUpperCase();
  if (expected.cursor_kind && cursorKind !== String(expected.cursor_kind).trim().toUpperCase()) {
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  const action = String(payload.action || '').trim().toUpperCase();
  const expectedAction = String(expected.action || '').trim().toUpperCase();
  if (!['GENERATE', 'ISSUE'].includes(action) || (expectedAction && action !== expectedAction)) {
    throw invoiceBatchContractError('BATCH_CURSOR_ACTION_MISMATCH');
  }
  const snapshot = normaliseInvoiceBatchSnapshot(payload.snapshot, action);
  if (expected.snapshot
      && postgresJsonbTextForInvoiceBatch(snapshot)
        !== postgresJsonbTextForInvoiceBatch(normaliseInvoiceBatchSnapshot(expected.snapshot, action))) {
    throw invoiceBatchContractError('BATCH_CURSOR_SNAPSHOT_MISMATCH');
  }
  const filterHash = String(payload.filter_hash || '').trim().toLowerCase();
  const queryHash = String(payload.query_hash || '').trim().toLowerCase();
  if (!SHA256_PATTERN.test(filterHash) || !SHA256_PATTERN.test(queryHash)) {
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  if (expected.filter_hash && filterHash !== String(expected.filter_hash).trim().toLowerCase()) {
    throw invoiceBatchContractError('BATCH_CURSOR_FILTER_MISMATCH');
  }
  if (expected.query_hash && queryHash !== String(expected.query_hash).trim().toLowerCase()) {
    throw invoiceBatchContractError('BATCH_CURSOR_QUERY_MISMATCH');
  }
  const sort = normaliseInvoiceBatchSort({ sort: payload.sort || {} }, action);
  if (expected.sort) {
    const expectedSort = normaliseInvoiceBatchSort({ sort: expected.sort }, action);
    if (postgresJsonbTextForInvoiceBatch(sort) !== postgresJsonbTextForInvoiceBatch(expectedSort)) {
      throw invoiceBatchContractError('BATCH_CURSOR_SORT_MISMATCH');
    }
  }
  const issuedAt = new Date(payload.issued_at_utc);
  const expiresAt = new Date(payload.expires_at_utc);
  const configuredTtl = Number(env?.INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS);
  const maximumTtlSeconds = Number.isSafeInteger(configuredTtl) && configuredTtl > 0
    ? Math.min(configuredTtl, INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS)
    : INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS;
  if (!Number.isFinite(issuedAt.getTime()) || !Number.isFinite(expiresAt.getTime())
      || expiresAt <= issuedAt || expiresAt > new Date(snapshot.expires_at_utc)) {
    throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
  }
  if (issuedAt.getTime() > Date.now() + 60_000
      || expiresAt.getTime() - issuedAt.getTime() > maximumTtlSeconds * 1000) {
    throw invoiceBatchContractError('BATCH_CURSOR_ISSUED_AT_INVALID');
  }
  if (Date.now() >= expiresAt.getTime()) throw invoiceBatchContractError('BATCH_CURSOR_EXPIRED');
  const keyset = normaliseInvoiceBatchCursorValues(payload.keyset || {}, { kind: cursorKind });
  return {
    version: 2,
    contract_version: INVOICE_BATCH_CURSOR_CONTRACT,
    cursor_kind: cursorKind,
    action,
    snapshot,
    query_hash: queryHash,
    filter_hash: filterHash,
    sort,
    keyset,
    cursor: keyset,
    next_cursor_values: keyset,
    issued_at_utc: issuedAt.toISOString(),
    expires_at_utc: expiresAt.toISOString()
  };
}

async function encodeInvoiceBatchResultCursorV2(env, input) {
  const rootOperationId = String(input?.root_operation_id || '').trim().toLowerCase();
  const action = String(input?.action || '').trim().toUpperCase();
  const category = String(input?.result_category || 'ALL').trim().toUpperCase();
  const resultPageRevision = String(input?.result_page_revision ?? '').trim();
  if (!UUID_PATTERN.test(rootOperationId)
      || !['GENERATE', 'ISSUE'].includes(action)
      || !INVOICE_OPERATION_RESULT_CATEGORIES.includes(category)
      || !/^[0-9]{1,18}$/.test(resultPageRevision)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  const afterSelectionKey = String(input?.next_cursor_values?.after_selection_key || '').trim();
  const afterChunkId = String(input?.next_cursor_values?.after_chunk_id || '').trim().toLowerCase();
  if (!afterSelectionKey || afterSelectionKey.length > 512 || !UUID_PATTERN.test(afterChunkId)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  const issuedAt = new Date(input?.issued_at_utc || Date.now());
  if (!Number.isFinite(issuedAt.getTime())) throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  const configuredTtl = Number(env?.INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS);
  const ttlSeconds = Number.isSafeInteger(configuredTtl) && configuredTtl > 0
    ? Math.min(configuredTtl, INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS)
    : INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS;
  const payload = {
    version: 2,
    contract_version: INVOICE_BATCH_RESULT_CURSOR_CONTRACT,
    root_operation_id: rootOperationId,
    action,
    result_category: category,
    result_page_revision: resultPageRevision,
    keyset: {
      after_selection_key: afterSelectionKey,
      after_chunk_id: afterChunkId
    },
    issued_at_utc: issuedAt.toISOString(),
    expires_at_utc: new Date(issuedAt.getTime() + ttlSeconds * 1000).toISOString()
  };
  const encodedPayload = encodeOutboxCursorPart(new TextEncoder().encode(
    postgresJsonbTextForInvoiceBatch(payload)
  ));
  const signature = await signInvoiceBatchCursor(
    cursorSecret(env, 'RESULT'),
    'invoice-batch-result-cursor-v2',
    encodedPayload
  );
  const token = `${encodedPayload}.${signature}`;
  if (token.length > INVOICE_BATCH_CURSOR_MAX_BYTES) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  return token;
}

async function decodeInvoiceBatchResultCursorV2(env, token, expected = {}) {
  const tokenText = String(token || '');
  if (!tokenText || tokenText.length > INVOICE_BATCH_CURSOR_MAX_BYTES) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  const parts = tokenText.split('.');
  if (parts.length !== 2 || parts.some(part => !part)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  let expectedSignature;
  let actualSignature;
  try {
    expectedSignature = decodeCanonicalInvoiceCursorPart(await signInvoiceBatchCursor(
      cursorSecret(env, 'RESULT'),
      'invoice-batch-result-cursor-v2',
      parts[0]
    ), 'OPERATION_RESULT_CURSOR_INVALID');
    actualSignature = decodeCanonicalInvoiceCursorPart(
      parts[1],
      'OPERATION_RESULT_CURSOR_INVALID'
    );
  } catch (error) {
    if (error?.code === 'BATCH_CURSOR_SECRET_INVALID') throw error;
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  if (expectedSignature.byteLength !== actualSignature.byteLength) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  let difference = 0;
  for (let index = 0; index < expectedSignature.byteLength; index += 1) {
    difference |= expectedSignature[index] ^ actualSignature[index];
  }
  if (difference !== 0) throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  let payload;
  try {
    payload = JSON.parse(new TextDecoder().decode(
      decodeCanonicalInvoiceCursorPart(parts[0], 'OPERATION_RESULT_CURSOR_INVALID')
    ));
  } catch {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  if (payload?.version !== 2
      || payload?.contract_version !== INVOICE_BATCH_RESULT_CURSOR_CONTRACT) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  const rootOperationId = String(payload.root_operation_id || '').trim().toLowerCase();
  const action = String(payload.action || '').trim().toUpperCase();
  const category = String(payload.result_category || '').trim().toUpperCase();
  const resultPageRevision = String(payload.result_page_revision ?? '').trim();
  if (!UUID_PATTERN.test(rootOperationId)
      || !['GENERATE', 'ISSUE'].includes(action)
      || !INVOICE_OPERATION_RESULT_CATEGORIES.includes(category)
      || !/^[0-9]{1,18}$/.test(resultPageRevision)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  if (expected.root_operation_id
      && rootOperationId !== String(expected.root_operation_id).trim().toLowerCase()) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  if (expected.action && action !== String(expected.action).trim().toUpperCase()) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  if (expected.result_category
      && category !== String(expected.result_category).trim().toUpperCase()) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  if (expected.result_page_revision != null
      && resultPageRevision !== String(expected.result_page_revision)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_STALE');
  }
  const issuedAt = new Date(payload.issued_at_utc);
  const expiresAt = new Date(payload.expires_at_utc);
  const configuredTtl = Number(env?.INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS);
  const maximumTtlSeconds = Number.isSafeInteger(configuredTtl) && configuredTtl > 0
    ? Math.min(configuredTtl, INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS)
    : INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS;
  if (!Number.isFinite(issuedAt.getTime()) || !Number.isFinite(expiresAt.getTime())
      || expiresAt <= issuedAt
      || issuedAt.getTime() > Date.now() + 60_000
      || expiresAt.getTime() - issuedAt.getTime() > maximumTtlSeconds * 1000) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  if (Date.now() >= expiresAt.getTime()) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_EXPIRED');
  }
  const keyset = plainInvoiceBatchObject(payload.keyset, 'OPERATION_RESULT_CURSOR_INVALID');
  if (Object.keys(keyset).some(key => !['after_selection_key', 'after_chunk_id'].includes(key))) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  const afterSelectionKey = String(keyset.after_selection_key || '').trim();
  const afterChunkId = String(keyset.after_chunk_id || '').trim().toLowerCase();
  if (!afterSelectionKey || afterSelectionKey.length > 512 || !UUID_PATTERN.test(afterChunkId)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  return {
    root_operation_id: rootOperationId,
    action,
    result_category: category,
    result_page_revision: resultPageRevision,
    keyset: {
      after_selection_key: afterSelectionKey,
      after_chunk_id: afterChunkId
    },
    issued_at_utc: issuedAt.toISOString(),
    expires_at_utc: expiresAt.toISOString()
  };
}

async function readInvoiceBatchJsonBody(req, options = {}) {
  const configured = Number(options.maximumBytes);
  const maximumBytes = Number.isSafeInteger(configured) && configured > 0
    ? Math.min(configured, INVOICE_BATCH_REQUEST_MAX_BYTES)
    : INVOICE_BATCH_REQUEST_MAX_BYTES;
  const contentLength = req.headers.get('content-length');
  if (contentLength && (!/^[0-9]+$/.test(contentLength) || Number(contentLength) > maximumBytes)) {
    throw invoiceBatchContractError('BATCH_REQUEST_TOO_LARGE');
  }
  const reader = req.body?.getReader();
  const chunks = [];
  let length = 0;
  if (reader) {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
      length += bytes.byteLength;
      if (length > maximumBytes) {
        try { await reader.cancel('BATCH_REQUEST_TOO_LARGE'); } catch {}
        throw invoiceBatchContractError('BATCH_REQUEST_TOO_LARGE');
      }
      chunks.push(bytes);
    }
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  let parsed;
  try {
    parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  } catch {
    throw invoiceBatchContractError('BATCH_REQUEST_JSON_INVALID');
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw invoiceBatchContractError('BATCH_REQUEST_OBJECT_REQUIRED');
  }
  return parsed;
}
function candidateGroupsFromRpc(value) {
  const isV2Envelope = candidate => !!(
    candidate
    && typeof candidate === 'object'
    && !Array.isArray(candidate)
    && candidate.contract_version === INVOICE_BATCH_CANDIDATE_CONTRACT
    && Array.isArray(candidate.rows)
  );

  const unwrapV2Envelope = candidate => {
    if (isV2Envelope(candidate)) return candidate;

    if (Array.isArray(candidate)) {
      if (candidate.length !== 1) return null;
      if (isV2Envelope(candidate[0])) return candidate[0];
      if (candidate[0] && typeof candidate[0] === 'object' && !Array.isArray(candidate[0])) {
        const innerValues = Object.values(candidate[0]);
        if (innerValues.length === 1) return unwrapV2Envelope(innerValues[0]);
      }
      return null;
    }

    if (candidate && typeof candidate === 'object') {
      if (Array.isArray(candidate.rows)) {
        const rowEnvelope = unwrapV2Envelope(candidate.rows);
        if (rowEnvelope) return rowEnvelope;
      }
      if (Array.isArray(candidate.data)) {
        const dataEnvelope = unwrapV2Envelope(candidate.data);
        if (dataEnvelope) return dataEnvelope;
      }
      const innerValues = Object.values(candidate);
      if (innerValues.length === 1) return unwrapV2Envelope(innerValues[0]);
    }

    return null;
  };

  const envelope = unwrapV2Envelope(value) || unwrapV2Envelope(rpcValue(value));
  if (!envelope) {
    throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
  }
  const action = String(envelope.action || '').trim().toUpperCase();
  const mode = String(envelope.mode || '').trim().toUpperCase();
  const nonNegativeInteger = value => Number.isSafeInteger(Number(value)) && Number(value) >= 0;
  const page = envelope.page;
  const totals = envelope.totals;
  const selectionSummary = envelope.selection_summary;
  if (!['GENERATE', 'ISSUE'].includes(action)
      || !['PAGE', 'FACETS', 'SUMMARY', 'EXPLICIT_KEYS', 'EXPAND_SELECTION'].includes(mode)
      || !page || typeof page !== 'object' || Array.isArray(page)
      || !totals || typeof totals !== 'object' || Array.isArray(totals)
      || !selectionSummary || typeof selectionSummary !== 'object' || Array.isArray(selectionSummary)
      || !Array.isArray(envelope.group_selection)
      || !envelope.facets || typeof envelope.facets !== 'object' || Array.isArray(envelope.facets)
      || !SHA256_PATTERN.test(String(envelope.filter_hash || '').toLowerCase())
      || !SHA256_PATTERN.test(String(envelope.query_hash || '').toLowerCase())
      || !SHA256_PATTERN.test(String(envelope.selection_hash || '').toLowerCase())
      || !nonNegativeInteger(page.page_size)
      || !nonNegativeInteger(page.returned_count)
      || !nonNegativeInteger(page.total_count)
      || Number(page.returned_count) !== envelope.rows.length
      || Number(page.returned_count) > Number(page.page_size)
      || typeof page.has_more !== 'boolean'
      || !['filtered_total', 'display_total', 'eligible_total', 'selected_total', 'excluded_total', 'blocked_total']
        .every(field => nonNegativeInteger(totals[field]))
      || !['eligible_total', 'selected_total', 'excluded_total', 'blocked_total']
        .every(field => nonNegativeInteger(selectionSummary[field]))
      || typeof selectionSummary.exact !== 'boolean'
      || (mode === 'PAGE' && selectionSummary.exact !== false)
      || (mode === 'SUMMARY' && selectionSummary.exact !== true)) {
    throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
  }

  const allowedGroupFields = new Set([
    'selector', 'group_key', 'eligible_total', 'selected_total',
    'state', 'has_hidden_override'
  ]);
  const groupIdentities = new Set();
  const normalizedGroups = [];
  try {
    if (!['PAGE', 'SUMMARY'].includes(mode) && envelope.group_selection.length !== 0) {
      throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
    }
    for (const group of envelope.group_selection) {
      if (!group || typeof group !== 'object' || Array.isArray(group)
          || Object.keys(group).length !== allowedGroupFields.size
          || [...allowedGroupFields].some(field =>
            !Object.prototype.hasOwnProperty.call(group, field))
          || !group.selector || typeof group.selector !== 'object' || Array.isArray(group.selector)
          || !Number.isSafeInteger(group.eligible_total)
          || group.eligible_total < 0
          || !Number.isSafeInteger(group.selected_total)
          || group.selected_total < 0
          || group.selected_total > group.eligible_total
          || typeof group.has_hidden_override !== 'boolean') {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
      const selector = normaliseInvoiceBatchGroupSelectors([group.selector])[0];
      if (postgresJsonbTextForInvoiceBatch(selector)
          !== postgresJsonbTextForInvoiceBatch(group.selector)) {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
      const identity = postgresJsonbTextForInvoiceBatch(selector);
      if (groupIdentities.has(identity)) {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
      groupIdentities.add(identity);

      if (group.group_key !== null && typeof group.group_key !== 'string') {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
      const groupKey = group.group_key == null ? null : group.group_key.trim();
      if ((mode === 'PAGE' && !groupKey)
          || (groupKey != null && (!groupKey || groupKey.length > 512))) {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
      const eligibleTotal = group.eligible_total;
      const selectedTotal = group.selected_total;
      const state = String(group.state || '').trim().toUpperCase();
      const expectedState = eligibleTotal === 0
        ? 'DISABLED'
        : selectedTotal === 0
          ? 'UNCHECKED'
          : selectedTotal === eligibleTotal && group.has_hidden_override !== true
            ? 'CHECKED'
            : 'INDETERMINATE';
      if (state !== expectedState
          || (state === 'DISABLED'
            && (selectedTotal !== 0 || group.has_hidden_override !== false))) {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
      normalizedGroups.push({
        selector,
        group_key: groupKey,
        eligible_total: eligibleTotal,
        selected_total: selectedTotal,
        state,
        has_hidden_override: group.has_hidden_override
      });
    }
  } catch {
    throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
  }
  return {
    ...envelope,
    group_selection: normalizedGroups,
    kind: 'V2',
    legacy: false,
    action,
    mode,
    normalised_filter: envelope.normalised_filter || envelope.normalized_filter || {},
    normalised_sort: envelope.normalised_sort || envelope.normalized_sort || {}
  };
}

async function startCommands(env, req, ctx, user, commands, deps, lanes = ['ALL'], options = {}) {
  const raw = await deps.rpc('invoice_operation_start_batch', {
    p_commands: commands,
    p_actor_user_id: user.id
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
  const isSelectionRoot = row => row?.selection_contract_version === INVOICE_BATCH_SELECTION_CONTRACT
    || row?.selection_expansion_pending === true
    || row?.input_json?.contract_version === INVOICE_BATCH_SELECTION_ROOT_CONTRACT;

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
    per_command_results: options.selectionRoot === true ? operations.map(row => ({
      command_no: row?.command_no ?? null,
      command_type: row?.command_type || null,
      accepted: row?.accepted === true,
      operation_id: row?.operation_id || null,
      operation_type: row?.operation_type || null,
      status: row?.status || null,
      phase: row?.phase || null,
      change_seq: row?.change_seq ?? null,
      created: row?.created === true,
      reused_active: row?.reused_active === true,
      reused_ready: row?.reused_ready === true,
      blocked: row?.blocked === true,
      selection_expansion_pending: row?.selection_expansion_pending === true,
      terminal_error: row?.terminal_error?.code
        ? { code: String(row.terminal_error.code).slice(0, 160) }
        : null
    })) : operations,
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

function normaliseInvoiceBatchQueryBody(body, action, options = {}) {
  const request = plainInvoiceBatchObject(body);
  const mode = validateInvoiceBatchSourceFields(request);
  if (request.contract_version !== INVOICE_BATCH_QUERY_CONTRACT) {
    throw invoiceBatchContractError('INVOICE_BATCH_QUERY_CONTRACT_INVALID');
  }
  const normalizedAction = String(request.action || '').trim().toUpperCase();
  if (normalizedAction !== action) throw invoiceBatchContractError('INVOICE_BATCH_QUERY_ACTION_MISMATCH');
  const filters = normaliseInvoiceBatchFilters(request, action, options);
  const sort = normaliseInvoiceBatchSort(request, action);
  const selection = normaliseInvoiceBatchSelectionRules(request.selection);
  const snapshot = normaliseInvoiceBatchSnapshot(request.snapshot, action, {
    allowNull: mode === 'PAGE' && !request.cursor
  });
  const query = {
    contract_version: INVOICE_BATCH_QUERY_CONTRACT,
    action,
    mode,
    snapshot,
    filters,
    sort,
    selection
  };
  if (mode === 'PAGE') {
    const pageSize = Number(request.page_size ?? 100);
    if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 100) {
      throw invoiceBatchContractError('INVOICE_BATCH_PAGE_SIZE_INVALID');
    }
    if (request.cursor != null && typeof request.cursor !== 'string') {
      throw invoiceBatchContractError('BATCH_CURSOR_INVALID');
    }
    if (request.cursor && !snapshot) throw invoiceBatchContractError('BATCH_SNAPSHOT_REQUIRED');
    query.page_size = pageSize;
    query.cursor = request.cursor || null;
  } else if (mode === 'FACETS') {
    if (!snapshot) throw invoiceBatchContractError('BATCH_SNAPSHOT_REQUIRED');
    query.facet_request = normaliseInvoiceBatchFacetRequest(request.facet_request);
  } else if (mode === 'SUMMARY') {
    if (!snapshot) throw invoiceBatchContractError('BATCH_SNAPSHOT_REQUIRED');
    query.group_selectors = normaliseInvoiceBatchGroupSelectors(request.group_selectors);
  } else if (mode === 'EXPLICIT_KEYS') {
    if (!snapshot) throw invoiceBatchContractError('BATCH_SNAPSHOT_REQUIRED');
    Object.assign(query, normaliseInvoiceBatchExplicitKeys(request, {
      maximum: options.explicitKeysMaximum ?? 1,
      exactCount: options.explicitKeysExactCount ?? 1
    }));
  }
  return query;
}

async function handleCandidates(env, req, deps, rpcName, options = {}) {
  const action = rpcName === 'invoice_batch_issue_candidates' ? 'ISSUE' : 'GENERATE';
  if (req.method !== 'POST') {
    return jsonResponse({ ok: false, error: 'BATCH_QUERY_POST_REQUIRED' }, 405, { allow: 'POST' });
  }
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  const query = normaliseInvoiceBatchQueryBody(body, action, options);
  const expectedFilterHash = await hashInvoiceBatchFilter(action, query.filters, query.sort);
  let expectedQueryHash = query.snapshot
    ? await hashInvoiceBatchQuery(action, query.filters, query.sort, query.snapshot)
    : null;

  if (query.mode === 'PAGE' && query.cursor) {
    const decoded = await decodeInvoiceBatchCursor(env, query.cursor, {
      cursor_kind: 'PAGE',
      action,
      snapshot: query.snapshot,
      filter_hash: expectedFilterHash,
      query_hash: expectedQueryHash,
      sort: query.sort
    });
    query.cursor = decoded.keyset;
  } else if (query.mode === 'PAGE') {
    query.cursor = null;
  }

  if (query.mode === 'FACETS') {
    const rawCursors = {};
    const names = {
      clients: 'CLIENTS',
      candidates: 'CANDIDATES',
      week_endings: 'WEEK_ENDINGS',
      statuses: 'STATUSES',
      blockers: 'BLOCKERS'
    };
    for (const [name, kind] of Object.entries(names)) {
      const token = query.facet_request.cursors[name];
      if (!token) continue;
      const decoded = await decodeInvoiceBatchCursor(env, token, {
        cursor_kind: kind,
        action,
        snapshot: query.snapshot,
        filter_hash: expectedFilterHash,
        query_hash: expectedQueryHash,
        sort: query.sort
      });
      rawCursors[name] = decoded.keyset;
    }
    query.facet_request = { ...query.facet_request, cursors: rawCursors };
  }

  let rawCandidateResponse;
  try {
    rawCandidateResponse = await deps.rpc(rpcName, { p_query: query });
  } catch (error) {
    const status = Number(error?.status || 0);
    const message = String(error?.message || error || '');
    if (status === 408 || /(?:statement\s+)?timeout|timed out/i.test(message)) {
      throw invoiceBatchContractError(
        query.mode === 'SUMMARY'
          ? 'BATCH_SUMMARY_TIMEOUT'
          : 'INVOICE_ASYNC_TEMPORARILY_UNAVAILABLE'
      );
    }
    throw error;
  }
  const parsed = candidateGroupsFromRpc(rawCandidateResponse);
  if (parsed.action !== action || parsed.mode !== query.mode) {
    throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
  }
  const responseSnapshot = normaliseInvoiceBatchSnapshot(parsed.snapshot, action);
  if (query.snapshot && postgresJsonbTextForInvoiceBatch(query.snapshot)
      !== postgresJsonbTextForInvoiceBatch(responseSnapshot)) {
    throw invoiceBatchContractError('BATCH_SNAPSHOT_QUERY_MISMATCH');
  }
  expectedQueryHash ||= await hashInvoiceBatchQuery(action, query.filters, query.sort, responseSnapshot);
  const expectedSelectionHash = await hashInvoiceBatchSelection(query.selection);
  if (parsed.filter_hash !== expectedFilterHash
      || parsed.query_hash !== expectedQueryHash
      || parsed.selection_hash !== expectedSelectionHash
      || postgresJsonbTextForInvoiceBatch(parsed.normalised_filter)
        !== postgresJsonbTextForInvoiceBatch(query.filters)
      || postgresJsonbTextForInvoiceBatch(parsed.normalised_sort)
        !== postgresJsonbTextForInvoiceBatch(query.sort)) {
    throw invoiceBatchContractError('BATCH_QUERY_HASH_MISMATCH');
  }

  let groupSelection = parsed.group_selection;
  if (query.mode === 'SUMMARY') {
    const requestedSelectors = normaliseInvoiceBatchGroupSelectors(query.group_selectors);
    const returnedByIdentity = new Map(groupSelection.map(group => [
      postgresJsonbTextForInvoiceBatch(group.selector),
      group
    ]));
    if (returnedByIdentity.size !== groupSelection.length
        || returnedByIdentity.size !== requestedSelectors.length) {
      throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
    }
    groupSelection = requestedSelectors.map(selector => {
      const group = returnedByIdentity.get(postgresJsonbTextForInvoiceBatch(selector));
      if (!group) {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
      return group;
    });
  }

  const page = { ...parsed.page, next_cursor: null };
  if (query.mode === 'PAGE' && page.has_more === true && !page.next_cursor_values) {
    throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
  }
  if (page.has_more === true && page.next_cursor_values && query.mode === 'PAGE') {
    page.next_cursor = await encodeInvoiceBatchCursor(env, {
      cursor_kind: 'PAGE',
      action,
      snapshot: responseSnapshot,
      filter_hash: expectedFilterHash,
      query_hash: expectedQueryHash,
      sort: query.sort,
      next_cursor_values: page.next_cursor_values
    });
  }
  delete page.next_cursor_values;

  const facets = structuredClone(parsed.facets || {});
  if (query.mode === 'FACETS') {
    const names = {
      clients: 'CLIENTS',
      candidates: 'CANDIDATES',
      week_endings: 'WEEK_ENDINGS',
      statuses: 'STATUSES',
      blockers: 'BLOCKERS'
    };
    const requestedNames = new Set(
      query.facet_request.kinds.map(kind =>
        Object.entries(names).find(([, candidateKind]) => candidateKind === kind)?.[0]
      )
    );
    for (const name of requestedNames) {
      const facet = facets[name];
      if (!facet || typeof facet !== 'object' || Array.isArray(facet)
          || !Array.isArray(facet.items)
          || facet.items.length > query.facet_request.limit_per_kind
          || typeof facet.has_more !== 'boolean'
          || (facet.has_more === true && !facet.next_cursor_values)) {
        throw invoiceBatchContractError('INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
      }
    }
    for (const [name, kind] of Object.entries(names)) {
      const facet = facets[name];
      if (!facet || typeof facet !== 'object' || Array.isArray(facet)) continue;
      facet.next_cursor = null;
      if (facet.has_more === true && facet.next_cursor_values) {
        facet.next_cursor = await encodeInvoiceBatchCursor(env, {
          cursor_kind: kind,
          action,
          snapshot: responseSnapshot,
          filter_hash: expectedFilterHash,
          query_hash: expectedQueryHash,
          sort: query.sort,
          next_cursor_values: facet.next_cursor_values
        });
      }
      delete facet.next_cursor_values;
    }
  }

  const {
    invoice_streams: _internalInvoiceStreams,
    ...browserNormalisedFilter
  } = query.filters;

  return jsonResponse({
    ok: true,
    contract_version: INVOICE_BATCH_CANDIDATE_CONTRACT,
    action,
    mode: query.mode,
    snapshot: responseSnapshot,
    query_hash: expectedQueryHash,
    filter_hash: expectedFilterHash,
    selection_hash: expectedSelectionHash,
    rows: parsed.rows,
    page,
    totals: parsed.totals,
    selection_summary: parsed.selection_summary,
    group_selection: groupSelection,
    facets,
    normalised_filter: browserNormalisedFilter,
    normalised_sort: query.sort,
    selection_seed: parsed.selection_seed || {
      mode: 'IMPLICIT_ALL',
      default_selected: true
    }
  });
}

async function handleNhspCandidates(env, req, deps) {
  return handleCandidates(env, req, deps, 'invoice_batch_generate_candidates', {
    forcedInvoiceStreams: ['NHSP'],
    allowInvoiceStreams: true
  });
}

function normaliseInvoiceBatchSelectionRoot(value, action) {
  const root = plainInvoiceBatchObject(value, 'BATCH_SELECTION_INVALID');
  if (Object.keys(root).some(key => !['contract_version', 'query', 'selection'].includes(key))
      || root.contract_version !== INVOICE_BATCH_SELECTION_ROOT_CONTRACT) {
    throw invoiceBatchContractError('BATCH_SELECTION_CONTRACT_INVALID');
  }
  const selection = normaliseInvoiceBatchSelectionRules(root.selection);
  const rawQuery = plainInvoiceBatchObject(root.query, 'BATCH_QUERY_INVALID');
  const query = normaliseInvoiceBatchQueryBody({
    ...rawQuery,
    selection
  }, action, { allowInvoiceStreams: true });
  return {
    contract_version: INVOICE_BATCH_SELECTION_ROOT_CONTRACT,
    query,
    selection
  };
}

async function handleBatchGenerateConfirm(env, req, ctx, user, deps) {
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  if (Object.keys(body).some(key => ![
    'selection_contract', 'command_token'
  ].includes(key))) {
    throw invoiceBatchContractError('BATCH_QUERY_UNKNOWN_FIELD');
  }
  const token = commandToken(req, body, {
    requiredCode: 'GENERATE_COMMAND_TOKEN_REQUIRED',
    invalidCode: 'GENERATE_COMMAND_TOKEN_INVALID'
  });
  const selectionRoot = normaliseInvoiceBatchSelectionRoot(body.selection_contract, 'GENERATE');
  if (!selectionRoot.query.snapshot) throw invoiceBatchContractError('BATCH_SNAPSHOT_REQUIRED');
  if (selectionRoot.query.mode === 'EXPLICIT_KEYS') {
    if (selectionRoot.query.selection_keys.length !== 1) {
      throw invoiceBatchContractError('BATCH_EXPLICIT_KEYS_INVALID');
    }
    const parsed = candidateGroupsFromRpc(await deps.rpc('invoice_batch_generate_candidates', {
      p_query: selectionRoot.query
    }));
    if (parsed.action !== 'GENERATE' || parsed.mode !== 'EXPLICIT_KEYS'
        || parsed.rows.length !== 1 || parsed.rows[0]?.selectable !== true) {
      throw invoiceBatchContractError('BATCH_SOURCE_CHANGED');
    }
    const row = parsed.rows[0];
    const canonical = row.command_payload;
    if (!canonical || typeof canonical !== 'object' || Array.isArray(canonical)) {
      throw invoiceBatchContractError('CANONICAL_COMMAND_REQUIRED');
    }
    const canonicalCommandType = String(canonical.command_type || '').trim().toUpperCase();
    let command;
    if (canonicalCommandType === 'VIEW_INVOICE_DOCUMENT') {
      const expectedRevision = String(canonical.expected_revision ?? '').trim();
      const sourceRevision = String(canonical.source_revision ?? '').trim();
      if (
        String(canonical.purpose || '').trim().toUpperCase() !== 'DRAFT_PREVIEW'
        || !/^[1-9][0-9]*$/.test(expectedRevision)
        || sourceRevision !== expectedRevision
      ) {
        throw invoiceBatchContractError('CANONICAL_COMMAND_REQUIRED');
      }
      command = {
        command_type: 'VIEW_INVOICE_DOCUMENT',
        invoice_id: canonicalUuidArray([canonical.invoice_id])[0],
        purpose: 'DRAFT_PREVIEW',
        expected_revision: expectedRevision,
        source_revision: sourceRevision,
        priority_reason: 'VIEW_NOW',
        command_token: token
      };
    } else {
      if (canonicalCommandType !== 'GENERATE_SELECTED') {
        throw invoiceBatchContractError('CANONICAL_COMMAND_REQUIRED');
      }
      command = generationCommandFromBody(req, {
        canonical_command: canonical,
        command_token: token,
        allow_early: selectionRoot.query.filters.allow_early
      }, canonicalCommandType);
    }
    return startCommands(env, req, ctx, user, [
      command
    ], deps, ['DATABASE'], {
      priorityClass: 'VIEW_NOW',
      extendResult: (summary, rows) => ({
        mode: 'GENERATE_AND_VIEW',
        selection_key: row.selection_key,
        source_revision: row.source_revision,
        operation_id: rows[0]?.operation_id || summary.root_operation_id || null
      })
    });
  }
  if (!['PAGE', 'SUMMARY'].includes(selectionRoot.query.mode)) {
    throw invoiceBatchContractError('INVOICE_BATCH_QUERY_MODE_INVALID');
  }
  selectionRoot.query = {
    contract_version: INVOICE_BATCH_QUERY_CONTRACT,
    action: 'GENERATE',
    mode: 'PAGE',
    snapshot: selectionRoot.query.snapshot,
    page_size: 100,
    cursor: null,
    filters: selectionRoot.query.filters,
    sort: selectionRoot.query.sort,
    selection: selectionRoot.selection
  };
  return startCommands(env, req, ctx, user, [{
    command_type: 'GENERATE_SELECTED',
    selection_contract: selectionRoot,
    command_token: token
  }], deps, ['DATABASE'], {
    selectionRoot: true,
    priorityClass: 'INTERACTIVE',
    extendResult: (summary, operationRows) => {
      const root = operationRows[0] || {};
      return {
        contract_version: INVOICE_BATCH_SELECTION_ROOT_CONTRACT,
        progress_contract_version: INVOICE_BATCH_PROGRESS_CONTRACT,
        root_operation_id: root.operation_id || summary.root_operation_id || null,
        status: root.status || null,
        phase: root.phase || null,
        change_seq: root.change_seq ?? null,
        selection_expansion_pending: root.selection_expansion_pending === true,
        estimated_filtered_total: root.estimated_filtered_total ?? null,
        estimated_selected_total: root.estimated_selected_total ?? null
      };
    }
  });
}

async function handleBatchIssueConfirm(env, req, ctx, user, deps) {
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  if (Object.keys(body).some(key => ![
    'selection_contract', 'deliver', 'command_token',
    'delivery_request_token', 'delivery_intent'
  ].includes(key))) {
    throw invoiceBatchContractError('BATCH_QUERY_UNKNOWN_FIELD');
  }
  if (typeof body.deliver !== 'boolean') {
    throw invoiceBatchContractError('ISSUE_DELIVERY_MODE_REQUIRED');
  }
  const token = commandToken(req, body, {
    requiredCode: 'ISSUE_COMMAND_TOKEN_REQUIRED',
    invalidCode: 'ISSUE_COMMAND_TOKEN_INVALID'
  });
  const selectionRoot = normaliseInvoiceBatchSelectionRoot(body.selection_contract, 'ISSUE');
  if (!selectionRoot.query.snapshot) throw invoiceBatchContractError('BATCH_SNAPSHOT_REQUIRED');
  if (!['PAGE', 'SUMMARY'].includes(selectionRoot.query.mode)) {
    throw invoiceBatchContractError('INVOICE_BATCH_QUERY_MODE_INVALID');
  }
  selectionRoot.query = {
    contract_version: INVOICE_BATCH_QUERY_CONTRACT,
    action: 'ISSUE',
    mode: 'PAGE',
    snapshot: selectionRoot.query.snapshot,
    page_size: 100,
    cursor: null,
    filters: selectionRoot.query.filters,
    sort: selectionRoot.query.sort,
    selection: selectionRoot.selection
  };
  let deliveryRequestToken = null;
  let deliveryIntent = {};
  if (body.deliver) {
    deliveryRequestToken = normaliseDeliveryRequestToken(
      body.delivery_request_token,
      token
    );
    const suppliedIntent = plainInvoiceBatchObject(
      body.delivery_intent,
      'ISSUE_DELIVERY_INTENT_INVALID'
    );
    if (Object.keys(suppliedIntent).some(key => !['route_mode', 'template_version'].includes(key))
        || String(suppliedIntent.route_mode || '').trim().toUpperCase() !== 'SERVER_RESOLVED'
        || String(suppliedIntent.template_version || '').trim() !== 'INVOICE_EMAIL_V2') {
      throw invoiceBatchContractError('ISSUE_DELIVERY_INTENT_INVALID');
    }
    deliveryIntent = {
      route_mode: 'SERVER_RESOLVED',
      template_version: 'INVOICE_EMAIL_V2'
    };
  } else if (body.delivery_request_token != null || body.delivery_intent != null) {
    throw invoiceBatchContractError('ISSUE_DELIVERY_INTENT_INVALID');
  }
  return startCommands(env, req, ctx, user, [{
    command_type: 'ISSUE_INVOICES',
    selection_contract: selectionRoot,
    deliver: body.deliver,
    command_token: token,
    delivery_intent: deliveryIntent,
    ...(deliveryRequestToken ? { delivery_request_token: deliveryRequestToken } : {})
  }], deps, ['DATABASE'], {
    selectionRoot: true,
    priorityClass: 'INTERACTIVE',
    extendResult: (summary, operationRows) => {
      const root = operationRows[0] || {};
      return {
        contract_version: INVOICE_BATCH_SELECTION_ROOT_CONTRACT,
        progress_contract_version: INVOICE_BATCH_PROGRESS_CONTRACT,
        root_operation_id: root.operation_id || summary.root_operation_id || null,
        status: root.status || null,
        phase: root.phase || null,
        change_seq: root.change_seq ?? null,
        selection_expansion_pending: root.selection_expansion_pending === true,
        estimated_filtered_total: root.estimated_filtered_total ?? null,
        estimated_selected_total: root.estimated_selected_total ?? null,
        delivery_requested: body.deliver
      };
    }
  });
}

async function handleViewDocument(env, req, ctx, user, deps, entityType, entityId) {
  if (!UUID_PATTERN.test(entityId)) return jsonResponse({ error: 'INVALID_ENTITY_ID' }, 400);
  if (req.method !== 'POST') {
    return jsonResponse({ ok: false, error: 'DOCUMENT_PREPARATION_POST_REQUIRED' }, 405, {
      allow: 'POST'
    });
  }
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  const allowedFields = new Set([
    'command_token', 'priority_reason', 'template_version'
  ]);
  if (Object.keys(body).some(key => !allowedFields.has(key))) {
    throw invoiceBatchContractError('DOCUMENT_PREPARATION_REQUEST_INVALID');
  }
  const requestToken = commandToken(req, body, {
    requiredCode: 'DOCUMENT_PREPARATION_COMMAND_TOKEN_REQUIRED',
    invalidCode: 'DOCUMENT_PREPARATION_COMMAND_TOKEN_INVALID'
  });
  const priorityReason = String(body.priority_reason || 'VIEW_NOW').trim().toUpperCase();
  if (priorityReason !== 'VIEW_NOW') {
    throw invoiceBatchContractError('DOCUMENT_PREPARATION_REQUEST_INVALID');
  }
  const templateVersion = body.template_version == null
    ? undefined
    : String(body.template_version).trim();
  if (templateVersion !== undefined
      && (!templateVersion || templateVersion.length > 160
        || /[\u0000-\u001f\u007f]/.test(templateVersion))) {
    throw invoiceBatchContractError('DOCUMENT_PREPARATION_REQUEST_INVALID');
  }
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

  if (entityType === 'TIMESHEET') {
    const timesheetQuery = new URL(`${env.SUPABASE_URL}/rest/v1/timesheets`);
    timesheetQuery.searchParams.set('timesheet_id', `eq.${entityId}`);
    timesheetQuery.searchParams.set('is_current', 'eq.true');
    timesheetQuery.searchParams.set(
      'select',
      'timesheet_id,submission_mode,document_revision,manual_document_asset_id,manual_pdf_r2_key,manual_pdf_rotation_degrees'
    );
    timesheetQuery.searchParams.set('limit', '1');
    const timesheetResponse = await fetch(timesheetQuery, { headers: serviceHeaders });
    const timesheetRows = await timesheetResponse.json().catch(() => []);
    const timesheet = Array.isArray(timesheetRows) ? timesheetRows[0] : null;
    if (!timesheetResponse.ok || !timesheet) {
      return jsonResponse({ error: 'DOCUMENT_ENTITY_NOT_FOUND' }, 404);
    }

    const submissionMode = String(timesheet.submission_mode || '').trim().toUpperCase();
    if (['MANUAL', 'QR'].includes(submissionMode) && !timesheet.manual_document_asset_id) {
      const evidenceQuery = new URL(`${env.SUPABASE_URL}/rest/v1/timesheet_evidence`);
      evidenceQuery.searchParams.set('timesheet_id', `eq.${entityId}`);
      evidenceQuery.searchParams.set('kind', 'eq.TIMESHEET');
      evidenceQuery.searchParams.set('processing_state', 'neq.SUPERSEDED');
      evidenceQuery.searchParams.set(
        'select',
        'id,storage_key,source_revision,display_name,document_asset_id,processing_state,created_at'
      );
      evidenceQuery.searchParams.set('order', 'created_at.desc.nullslast,id.desc');
      evidenceQuery.searchParams.set('limit', '1');
      const evidenceResponse = await fetch(evidenceQuery, { headers: serviceHeaders });
      const evidenceRows = await evidenceResponse.json().catch(() => []);
      const evidence = evidenceResponse.ok && Array.isArray(evidenceRows)
        ? evidenceRows[0]
        : null;

      const originalR2Key = String(
        evidence?.storage_key || timesheet.manual_pdf_r2_key || ''
      ).trim().replace(/^\/+/, '');
      if (!originalR2Key) {
        return jsonResponse({
          contract_version: INVOICE_VIEWER_CONTRACT,
          viewer_state: 'BLOCKED',
          purpose: 'TIMESHEET',
          badge_codes: ['MANUAL_TIMESHEET_SOURCE_MISSING'],
          error_code: 'MANUAL_TIMESHEET_SOURCE_MISSING',
          status_message: 'The signed source timesheet is not available.'
        }, 409);
      }

      const originalObject = await env.R2?.head?.(originalR2Key);
      if (!originalObject) {
        return jsonResponse({
          contract_version: INVOICE_VIEWER_CONTRACT,
          viewer_state: 'BLOCKED',
          purpose: 'TIMESHEET',
          badge_codes: ['MANUAL_TIMESHEET_SOURCE_MISSING'],
          error_code: 'MANUAL_TIMESHEET_SOURCE_MISSING',
          status_message: 'The signed source timesheet is not available.'
        }, 409);
      }

      const sourceKind = evidence?.id ? 'TIMESHEET_EVIDENCE' : 'MANUAL_TIMESHEET';
      const sourceId = evidence?.id || entityId;
      const sourceRevision = String(
        evidence?.source_revision
          || `${sourceKind}:${sourceId}:${timesheet.document_revision}`
      ).trim();
      const assetValues = rpcValue(await deps.rpc('invoice_operation_start_batch', {
        p_commands: [{
          command_type: 'PREPARE_ASSET',
          source_kind: sourceKind,
          source_id: sourceId,
          source_revision: sourceRevision,
          original_r2_key: originalR2Key,
          original_filename: String(
            evidence?.display_name || `Signed timesheet ${entityId}`
          ).slice(0, 240),
          declared_media_type: String(
            originalObject.httpMetadata?.contentType || 'application/octet-stream'
          ).slice(0, 160),
          rotation_degrees: Number(timesheet.manual_pdf_rotation_degrees || 0)
        }],
        p_actor_user_id: user.id
      }));
      const assetResults = Array.isArray(assetValues) ? assetValues : [assetValues];
      const assetResult = assetResults[0] || {};
      if (assetResult.accepted === false || assetResult.blocked === true
          || assetResult.terminal_error) {
        const errorCode = String(
          assetResult?.terminal_error?.code
            || assetResult?.error?.code
            || assetResult?.terminal_error
            || assetResult?.error
            || 'MANUAL_TIMESHEET_ASSET_PREPARATION_BLOCKED'
        ).slice(0, 160);
        return jsonResponse({
          contract_version: INVOICE_VIEWER_CONTRACT,
          viewer_state: 'BLOCKED',
          purpose: 'TIMESHEET',
          badge_codes: [errorCode],
          error_code: errorCode,
          status_message: 'The signed source timesheet could not be prepared.'
        }, 409);
      }

      const activeAssetOperations = assetResults.filter(
        row => row?.accepted !== false && row?.operation_id
      );
      if (activeAssetOperations.length) {
        await nudgeInvoiceOperations(env, activeAssetOperations, {
          ctx,
          rpc: deps.rpc,
          lanes: ['DOCUMENT'],
          priorityClass: 'VIEW_NOW'
        });
      }
      return jsonResponse({
        contract_version: INVOICE_VIEWER_CONTRACT,
        viewer_state: 'PREPARING',
        operation_id: assetResult.operation_id || null,
        purpose: 'TIMESHEET',
        status_message: 'Preparing signed timesheet'
      }, 202);
    }
  }

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
          contract_version: INVOICE_VIEWER_CONTRACT,
          viewer_state: 'BLOCKED',
          purpose: 'FINAL_ISSUE',
          badge_codes: ['ISSUED_DOCUMENT_POINTER_MISSING'],
          error_code: 'ISSUED_DOCUMENT_POINTER_MISSING',
          status_message: 'The issued document is not available.'
        }, 409);
      }
      const version = await loadReadyVersion(issuedVersionId, 'FINAL_ISSUE');
      return version
        ? jsonResponse({
          contract_version: INVOICE_VIEWER_CONTRACT,
          viewer_state: 'READY',
          purpose: 'FINAL_ISSUE',
          document_version: redactVersion(version)
        })
        : jsonResponse({
          contract_version: INVOICE_VIEWER_CONTRACT,
          viewer_state: 'BLOCKED',
          purpose: 'FINAL_ISSUE',
          badge_codes: ['ISSUED_DOCUMENT_INTEGRITY_FAILURE'],
          error_code: 'ISSUED_DOCUMENT_INTEGRITY_FAILURE',
          status_message: 'The issued document could not be verified.'
        }, 409);
    }
  }

  const purpose = entityType === 'INVOICE' ? 'DRAFT_PREVIEW' : 'TIMESHEET';
  const command = {
    command_type: entityType === 'INVOICE' ? 'VIEW_INVOICE_DOCUMENT' : 'VIEW_TIMESHEET_DOCUMENT',
    [entityType === 'INVOICE' ? 'invoice_id' : 'timesheet_id']: entityId,
    purpose,
    priority_reason: priorityReason,
    template_version: templateVersion,
    command_token: requestToken
  };
  const operationsValue = rpcValue(await deps.rpc('invoice_operation_start_batch', {
    p_commands: [command],
    p_actor_user_id: user.id
  }));
  const operations = Array.isArray(operationsValue) ? operationsValue : [operationsValue];
  const result = operations[0] || {};

  if (result.accepted === true && result.reused_ready === true && result.document_version_id) {
    const version = await loadReadyVersion(result.document_version_id, purpose);
    return version
      ? jsonResponse({
        contract_version: INVOICE_VIEWER_CONTRACT,
        viewer_state: 'READY',
        purpose,
        document_version: redactVersion(version)
      })
      : jsonResponse({
        contract_version: INVOICE_VIEWER_CONTRACT,
        viewer_state: 'BLOCKED',
        purpose,
        badge_codes: ['READY_DOCUMENT_IDENTITY_INVALID'],
        error_code: 'READY_DOCUMENT_IDENTITY_INVALID',
        status_message: 'The document could not be verified.'
      }, 409);
  }

  if (result.accepted === false || result.blocked === true || result.terminal_error) {
    const errorCode = String(result?.terminal_error?.code || result?.error?.code || result?.terminal_error || result?.error || 'DOCUMENT_PREPARATION_BLOCKED').slice(0, 160);
    return jsonResponse({
      contract_version: INVOICE_VIEWER_CONTRACT,
      viewer_state: 'BLOCKED',
      purpose,
      badge_codes: [errorCode],
      error_code: errorCode,
      status_message: entityType === 'INVOICE'
        ? 'The preview could not be prepared.'
        : 'The timesheet document could not be prepared.'
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
    contract_version: INVOICE_VIEWER_CONTRACT,
    viewer_state: 'PREPARING',
    operation_id: result.operation_id || null,
    purpose,
    status_message: entityType === 'INVOICE' ? 'Preparing preview' : 'Preparing timesheet'
  }, 202);
}

async function handleIssueOne(env, req, ctx, user, deps, invoiceId) {
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  const allowedFields = new Set([
    'expected_revision', 'deliver', 'allow_early', 'command_token',
    'delivery_request_token', 'recipient_set', 'to', 'cc', 'bcc',
    'delivery_policy', 'template_version'
  ]);
  if (Object.keys(body).some(key => !allowedFields.has(key))) {
    throw invoiceBatchContractError('INVOICE_ISSUE_REQUEST_INVALID');
  }
  const expectedRevision = String(body.expected_revision ?? '').trim();
  if (!/^[1-9][0-9]*$/.test(expectedRevision)) {
    return jsonResponse({ error: 'EXPECTED_INVOICE_REVISION_REQUIRED' }, 400);
  }
  const canonicalInvoiceId = canonicalUuidArray([invoiceId])[0];
  if (typeof body.deliver !== 'boolean') {
    throw invoiceBatchContractError('ISSUE_DELIVERY_MODE_REQUIRED');
  }
  const deliver = body.deliver;
  const deliveryFields = [
    'delivery_request_token', 'recipient_set', 'to', 'cc', 'bcc',
    'delivery_policy', 'template_version'
  ];
  if (!deliver && deliveryFields.some(field => body[field] != null)) {
    throw invoiceBatchContractError('ISSUE_DELIVERY_INTENT_INVALID');
  }
  if (deliver && body.template_version != null
      && String(body.template_version).trim() !== 'INVOICE_EMAIL_V2') {
    throw invoiceBatchContractError('ISSUE_DELIVERY_INTENT_INVALID');
  }
  const requestToken = commandToken(req, body, {
    requiredCode: 'ISSUE_COMMAND_TOKEN_REQUIRED',
    invalidCode: 'ISSUE_COMMAND_TOKEN_INVALID'
  });
  const deliveryRequestToken = deliver
    ? normaliseDeliveryRequestToken(body.delivery_request_token, requestToken)
    : null;
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
      template_version: body.template_version || 'INVOICE_EMAIL_V2'
    } : {},
    ...(deliveryRequestToken ? { delivery_request_token: deliveryRequestToken } : {})
  }], deps, ['DATABASE', 'DOCUMENT']);
}

async function handleDeliverOne(env, req, ctx, user, deps, invoiceId) {
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  const allowedFields = new Set([
    'command_token', 'delivery_request_token', 'recipient_set', 'to', 'cc',
    'bcc', 'delivery_policy', 'template_version', 'delivery_part_number'
  ]);
  if (Object.keys(body).some(key => !allowedFields.has(key))
      || (body.template_version != null
        && String(body.template_version).trim() !== 'INVOICE_EMAIL_V2')) {
    throw invoiceBatchContractError('DELIVERY_REQUEST_INVALID');
  }
  const requestToken = commandToken(req, body, {
    requiredCode: 'DELIVERY_COMMAND_TOKEN_REQUIRED',
    invalidCode: 'DELIVERY_COMMAND_TOKEN_INVALID'
  });
  const deliveryRequestToken = normaliseDeliveryRequestToken(
    body.delivery_request_token,
    requestToken
  );
  const rawDeliveryPartNumber = body.delivery_part_number ?? 1;
  if (
    (typeof rawDeliveryPartNumber !== 'number'
      && !/^[1-9][0-9]*$/.test(String(rawDeliveryPartNumber)))
    || !Number.isSafeInteger(Number(rawDeliveryPartNumber))
    || Number(rawDeliveryPartNumber) < 1
    || Number(rawDeliveryPartNumber) > 100
  ) {
    throw invoiceBatchContractError('DELIVERY_PART_NUMBER_INVALID');
  }
  const deliveryPartNumber = Number(rawDeliveryPartNumber);
  return startCommands(env, req, ctx, user, [{
    command_type: 'DELIVER_INVOICES',
    invoice_ids: canonicalUuidArray([invoiceId]),
    recipient_set: canonicalEmailArray(body.recipient_set || body.to || []),
    cc: canonicalEmailArray(body.cc || []),
    bcc: canonicalEmailArray(body.bcc || []),
    delivery_policy: canonicalDeliveryPolicy(body.delivery_policy),
    delivery_template_version: body.template_version || 'INVOICE_EMAIL_V2',
    delivery_part_number: deliveryPartNumber,
    command_token: requestToken,
    delivery_request_token: deliveryRequestToken
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
  const body = req.method === 'POST'
    ? await readInvoiceBatchJsonBody(req, {
      maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
    })
    : {};
  const ids = operationId
    ? canonicalUuidArray([operationId])
    : canonicalUuidArray(body.operation_ids || String(url.searchParams.get('operation_ids') || '').split(',').filter(Boolean));
  if (ids.length > 100) throw invoiceBatchContractError('OPERATION_RESULT_ROOT_INVALID');
  const mode = String(body.mode || url.searchParams.get('mode') || 'PROGRESS').toUpperCase();
  const categoryRaw = body.result_category || body.category || url.searchParams.get('result_category') || url.searchParams.get('category');
  const resultCategory = categoryRaw ? String(categoryRaw).trim().toUpperCase() : null;
  if (resultCategory && !INVOICE_OPERATION_RESULT_CATEGORIES.includes(resultCategory)) {
    throw invoiceBatchContractError('OPERATION_RESULT_CATEGORY_INVALID');
  }
  const resultCursorToken = body.result_cursor || body.cursor || url.searchParams.get('result_cursor') || url.searchParams.get('cursor');
  const resultLimitRaw = Number(body.result_limit || body.limit || url.searchParams.get('result_limit') || url.searchParams.get('limit') || 100);
  if (!Number.isSafeInteger(resultLimitRaw) || resultLimitRaw < 1 || resultLimitRaw > 100) {
    throw invoiceBatchContractError('OPERATION_RESULT_PAGE_REQUEST_INVALID');
  }
  const resultLimit = resultLimitRaw;

  let pageRequest = null;
  let resultAction = String(body.action || url.searchParams.get('action') || '').trim().toUpperCase();
  const requestedResultPage = !!(resultCategory || resultCursorToken);
  if (requestedResultPage) {
    if (ids.length !== 1 || !['GENERATE', 'ISSUE'].includes(resultAction)) {
      throw invoiceBatchContractError('OPERATION_RESULT_ROOT_INVALID');
    }
    const category = resultCategory || 'ALL';
    let cursorValues = {};
    let resultPageRevision = String(body.result_page_revision ?? url.searchParams.get('result_page_revision') ?? '').trim();
    if (resultCursorToken) {
      const decoded = await decodeInvoiceBatchResultCursorV2(env, resultCursorToken, {
        root_operation_id: ids[0],
        action: resultAction,
        result_category: category,
        ...(resultPageRevision ? { result_page_revision: resultPageRevision } : {})
      });
      cursorValues = decoded.keyset;
      resultPageRevision = decoded.result_page_revision;
    }
    if (!/^[0-9]{1,18}$/.test(resultPageRevision)) {
      throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
    }
    pageRequest = {
      category,
      limit: resultLimit,
      result_page_revision: resultPageRevision,
      after_selection_key: cursorValues.after_selection_key || null,
      after_chunk_id: cursorValues.after_chunk_id || null
    };
  }

  const result = await deps.rpc('invoice_operation_get', {
    p_operation_ids: ids,
    p_actor_user_id: user.id,
    p_mode: mode,
    p_page_request: pageRequest
  });
  const value = rpcValue(result);
  const operations = Array.isArray(value) ? value : (Array.isArray(value?.operations) ? value.operations : []);
  const resultPage = value && !Array.isArray(value) ? value.result_page || null : null;
  if (pageRequest && (
    resultPage?.contract_version !== 'INVOICE_BATCH_RESULT_PAGE_V2'
    || String(resultPage?.root_operation_id || '').toLowerCase() !== ids[0]
    || String(resultPage?.result_page_revision) !== String(pageRequest.result_page_revision)
    || String(resultPage?.category || '').toUpperCase() !== pageRequest.category
    || !Array.isArray(resultPage?.rows)
  )) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_STALE');
  }
  if (pageRequest && resultPage?.has_more === true && !resultPage?.next_cursor_values) {
    throw invoiceBatchContractError('OPERATION_RESULT_CURSOR_INVALID');
  }
  let signedNextResultCursor = null;
  if (resultPage?.has_more && resultPage?.next_cursor_values && pageRequest) {
    signedNextResultCursor = await encodeInvoiceBatchResultCursorV2(env, {
      root_operation_id: ids[0],
      action: resultAction,
      result_category: pageRequest.category,
      result_page_revision: pageRequest.result_page_revision,
      next_cursor_values: resultPage.next_cursor_values
    });
  }

  return jsonResponse({
    ok: true,
    operations,
    ...(resultPage ? {
      result_page: {
        ...resultPage,
        next_cursor_values: undefined,
        next_cursor: signedNextResultCursor
      }
    } : {})
  });
}

function normaliseInvoiceOperationControlAction(raw, options = {}) {
  const source = plainInvoiceBatchObject(
    raw,
    'OPERATION_CONTROL_ACTION_SCHEMA_INVALID'
  );
  const allowed = new Set(['RETRY','CANCEL','RESCHEDULE','RAISE_PRIORITY']);
  const action = String(source.action || '').trim().toUpperCase();
  const operationId = String(source.operation_id || '').trim().toLowerCase();
  if (!allowed.has(action) || !UUID_PATTERN.test(operationId)) {
    throw invoiceBatchContractError('OPERATION_CONTROL_ACTION_SCHEMA_INVALID');
  }
  const allowedFields = new Set(['action', 'operation_id']);
  if (action === 'RETRY') {
    allowedFields.add('retry_chunk_id');
    if (options.internal === true) allowedFields.add('replacement');
  } else if (action === 'RESCHEDULE') {
    allowedFields.add('run_after_utc');
  } else if (action === 'RAISE_PRIORITY' && options.internal === true) {
    allowedFields.add('priority');
  }
  if (Object.keys(source).some(key => !allowedFields.has(key))) {
    throw invoiceBatchContractError('OPERATION_CONTROL_ACTION_SCHEMA_INVALID');
  }
  const item = { operation_id: operationId, action };
  if (action === 'RETRY' && source.retry_chunk_id != null) {
    const retryChunkId = String(source.retry_chunk_id).trim().toLowerCase();
    if (!UUID_PATTERN.test(retryChunkId)) {
      throw invoiceBatchContractError('OPERATION_CONTROL_ACTION_SCHEMA_INVALID');
    }
    item.retry_chunk_id = retryChunkId;
  }
  if (action === 'RETRY' && options.internal === true && source.replacement != null) {
    item.replacement = structuredClone(plainInvoiceBatchObject(
      source.replacement,
      'OPERATION_CONTROL_ACTION_SCHEMA_INVALID'
    ));
  }
  if (action === 'RESCHEDULE') {
    const timestamp = new Date(source.run_after_utc || '');
    const now = Date.now();
    if (!Number.isFinite(timestamp.getTime())
        || timestamp.getTime() <= now
        || timestamp.getTime() > now + 30 * 24 * 60 * 60 * 1000) {
      throw invoiceBatchContractError('OPERATION_CONTROL_ACTION_SCHEMA_INVALID');
    }
    item.run_after_utc = timestamp.toISOString();
  }
  if (action === 'RAISE_PRIORITY' && options.internal === true
      && source.priority != null) {
    const priority = Number(source.priority);
    if (!Number.isSafeInteger(priority) || priority < 0 || priority > 2000) {
      throw invoiceBatchContractError('OPERATION_CONTROL_ACTION_SCHEMA_INVALID');
    }
    item.priority = priority;
  }
  return item;
}

function operationControlRequestToken(req, body) {
  return commandToken(req, body, {
    bodyFields: ['command_token', 'request_token'],
    requiredCode: 'OPERATION_CONTROL_REQUEST_TOKEN_REQUIRED',
    invalidCode: 'OPERATION_CONTROL_REQUEST_TOKEN_INVALID'
  });
}

async function invoiceOperationControlEnvelope(userId, requestToken, actions) {
  const canonicalPayload = {
    contract_version: 'INVOICE_OPERATION_CONTROL_V2',
    request_token: requestToken,
    actor_user_id: String(userId || '').trim().toLowerCase(),
    actions
  };
  return {
    contract_version: 'INVOICE_OPERATION_CONTROL_V2',
    request_token: requestToken,
    request_hash: await sha256Text(
      postgresJsonbTextForInvoiceBatch(canonicalPayload)
    ),
    actions
  };
}

function controlResultsReleasedRunnableWork(value) {
  const rows = Array.isArray(value) ? value : (value ? [value] : []);
  return rows.some(row =>
    row?.accepted === true
    && ['QUEUED','WAITING','RETRY_WAIT','RUNNING'].includes(String(row?.status || '').toUpperCase())
  );
}

async function handleOperationControl(env, req, ctx, user, deps) {
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  const allowedFields = new Set([
    'contract_version', 'actions', 'command_token', 'request_token'
  ]);
  if (Object.keys(body).some(key => !allowedFields.has(key))
      || body.contract_version !== 'INVOICE_OPERATION_CONTROL_V2'
      || !Array.isArray(body.actions)
      || body.actions.length < 1
      || body.actions.length > 100) {
    throw invoiceBatchContractError('OPERATION_CONTROL_ACTION_SCHEMA_INVALID');
  }
  const requestToken = operationControlRequestToken(req, body);
  const actions = body.actions;
  const safeActions = actions.map(normaliseInvoiceOperationControlAction);
  const envelope = await invoiceOperationControlEnvelope(
    user.id,
    requestToken,
    safeActions
  );
  const operations = rpcValue(await deps.rpc('invoice_operation_control_batch', {
    p_actions: envelope,
    p_actor_user_id: user.id
  }));
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

function decodeCanonicalInvoiceCursorPart(value, errorCode) {
  const text = String(value || '');
  if (!text || !/^[A-Za-z0-9_-]+$/.test(text)) {
    throw invoiceBatchContractError(errorCode);
  }
  let bytes;
  try {
    bytes = decodeOutboxCursorPart(text);
  } catch {
    throw invoiceBatchContractError(errorCode);
  }
  if (encodeOutboxCursorPart(bytes) !== text) {
    throw invoiceBatchContractError(errorCode);
  }
  return bytes;
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
  if (operationType && /^[A-Z_]+$/.test(operationType)) {
    invoiceQuery.searchParams.set('operation_type', `eq.${operationType}`);
  } else {
    invoiceQuery.searchParams.set(
      'operation_type',
      'neq.OPERATION_CONTROL_REQUEST'
    );
  }
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
  if (operationType === 'OPERATION_CONTROL_REQUEST') {
    return jsonResponse({ error: 'INVALID_OUTBOX_OPERATION_TYPE' }, 400);
  }
  if (operationType && /^[A-Z_]+$/.test(operationType)) {
    query.searchParams.set('operation_type', `eq.${operationType}`);
  } else {
    query.searchParams.set('operation_type', 'neq.OPERATION_CONTROL_REQUEST');
  }
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
  const body = await readInvoiceBatchJsonBody(req, {
    maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES) || INVOICE_BATCH_REQUEST_MAX_BYTES
  });
  const normalizedAction = String(action || '').trim().toUpperCase();
  const allowedFields = new Set([
    'contract_version', 'command_token', 'request_token'
  ]);
  if (normalizedAction === 'RETRY') allowedFields.add('retry_chunk_id');
  if (normalizedAction === 'RESCHEDULE') allowedFields.add('run_after_utc');
  if (Object.keys(body).some(key => !allowedFields.has(key))
      || body.contract_version !== 'INVOICE_OPERATION_CONTROL_V2') {
    throw invoiceBatchContractError('OPERATION_CONTROL_ACTION_SCHEMA_INVALID');
  }
  const requestToken = operationControlRequestToken(req, body);
  const controlAction = normaliseInvoiceOperationControlAction({
    operation_id: operationId,
    action: normalizedAction,
    ...(normalizedAction === 'RETRY' && body.retry_chunk_id != null
      ? { retry_chunk_id: body.retry_chunk_id }
      : {}),
    ...(normalizedAction === 'RESCHEDULE'
      ? { run_after_utc: body.run_after_utc }
      : {})
  });
  const envelope = await invoiceOperationControlEnvelope(
    user.id,
    requestToken,
    [controlAction]
  );
  const result = await deps.rpc('invoice_operation_control_batch', {
    p_actions: envelope,
    p_actor_user_id: user.id
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

async function loadInvoiceAsyncDatabaseContract(env, deps, options = {}) {
  const cacheKey = [
    String(env?.SUPABASE_URL || ''),
    String(env?.INVOICE_ASYNC_EXPECTED_FUNCTION_MANIFEST || '')
  ].join('|');
  const now = Date.now();
  const cached = invoiceAsyncDatabaseContractCache.get(cacheKey);
  if (!options.force && cached && cached.expiresAt > now) return cached.value;
  try {
    const value = rpcValue(await deps.rpc('invoice_async_contract_get_v2', {}));
    const contract = Array.isArray(value) ? value[0] : value;
    if (!contract || typeof contract !== 'object' || Array.isArray(contract)) {
      throw invoiceBatchContractError('INVOICE_ASYNC_DATABASE_CONTRACT_INVALID');
    }
    invoiceAsyncDatabaseContractCache.set(cacheKey, {
      value: contract,
      expiresAt: now + INVOICE_BATCH_DATABASE_CONTRACT_CACHE_MS
    });
    return contract;
  } catch {
    invoiceAsyncDatabaseContractCache.delete(cacheKey);
    return null;
  }
}

function validateInvoiceAsyncDatabaseContract(env, contract) {
  const expectedManifest = String(env?.INVOICE_ASYNC_EXPECTED_FUNCTION_MANIFEST || '').trim().toLowerCase();
  const errors = [];
  if (!contract || typeof contract !== 'object' || Array.isArray(contract)) {
    errors.push('INVOICE_ASYNC_DATABASE_CONTRACT_UNAVAILABLE');
  } else {
    if (contract.contract_version !== INVOICE_ASYNC_DB_CONTRACT_VERSION) {
      errors.push('INVOICE_ASYNC_DATABASE_CONTRACT_MISMATCH');
    }
    if (contract.ready !== true) errors.push('INVOICE_ASYNC_DATABASE_NOT_READY');
    if (contract.candidate_query_contract !== INVOICE_BATCH_QUERY_CONTRACT
        || contract.candidate_response_contract !== INVOICE_BATCH_CANDIDATE_CONTRACT
        || contract.selection_contract !== INVOICE_BATCH_SELECTION_CONTRACT
        || contract.selection_root_contract !== INVOICE_BATCH_SELECTION_ROOT_CONTRACT
        || contract.progress_contract !== INVOICE_BATCH_PROGRESS_CONTRACT) {
      errors.push('INVOICE_ASYNC_DATABASE_COMPONENT_CONTRACT_MISMATCH');
    }
    if (!SHA256_PATTERN.test(expectedManifest)) {
      errors.push('INVOICE_ASYNC_EXPECTED_FUNCTION_MANIFEST_INVALID');
    } else if (String(contract.function_hash_manifest || '').toLowerCase() !== expectedManifest) {
      errors.push('INVOICE_ASYNC_FUNCTION_MANIFEST_MISMATCH');
    }
    if (contract.indexes_ready !== true
        || contract.snapshot_signing_ready !== true
        || contract.operation_control_idempotency_ready !== true) {
      errors.push('INVOICE_ASYNC_DATABASE_PREREQUISITE_MISSING');
    }
    for (const field of [
      'missing_function_count',
      'private_exposure_count',
      'forbidden_dependency_count',
      'public_candidate_dependency_count',
      'legacy_runtime_exposure_count'
    ]) {
      if (Number(contract[field]) !== 0) errors.push(`INVOICE_ASYNC_${field.toUpperCase()}_INVALID`);
    }
  }
  return { ok: errors.length === 0, errors, contract };
}

async function invoiceProcessorReady(env, options = {}) {
  if (String(env?.INVOICE_DOCUMENT_PROCESSOR_ENABLED || '').toLowerCase() !== 'true') return false;
  const binding = env?.INVOICE_DOCUMENT_PROCESSOR;
  const now = Date.now();
  const cached = binding && typeof binding === 'object'
    ? invoiceAsyncProcessorReadyCache.get(binding)
    : null;
  if (!options.force && cached && cached.expiresAt > now) return cached.ready;
  const result = await checkInvoiceDocumentProcessorReady(env, { timeoutMs: 5_000 });
  const ready = result.ok === true;
  if (binding && typeof binding === 'object') {
    invoiceAsyncProcessorReadyCache.set(binding, {
      ready,
      expiresAt: now + (ready ? 15_000 : 5_000)
    });
  }
  return ready;
}

async function handleInvoiceAsyncCapabilities(env, req, deps) {
  const user = await requireActor(env, req, deps, false);
  if (!user) return jsonResponse({ error: 'UNAUTHENTICATED' }, 401);
  const access = parseInvoiceAsyncAccessMode(env.INVOICE_ASYNC_ACCESS_MODE);
  const parsed = parseInvoiceAsyncAllowedUserIds(env.INVOICE_ASYNC_ALLOWED_USER_IDS);
  const pipelineEnabled = isInvoiceAsyncPipelineEnabled(env);
  const scheduledEnabled = String(env.INVOICE_ASYNC_SCHEDULED_ENABLED || '').toLowerCase() === 'true';
  const databaseContract = await loadInvoiceAsyncDatabaseContract(env, deps);
  const databaseValidation = validateInvoiceAsyncDatabaseContract(env, databaseContract);
  const runtimeValidation = validateQueueRuntimeConfiguration(env);
  const processorEnabled = await invoiceProcessorReady(env);
  const buildReady = !!String(env.INVOICE_ASYNC_BUILD_ID || '').trim();
  const bindingsReady = !!(
    env.INVOICE_QUEUE_DISPATCHER
    && env.R2
    && cursorSecret(env, 'CANDIDATE')
    && cursorSecret(env, 'RESULT')
    && env.INVOICE_DOCUMENT_ACCESS_SECRET
  );
  const accessConfigurationReady = access.ok && (
    access.mode === 'AUTHENTICATED'
    || (parsed.ok && parsed.ids.length > 0)
  );
  const deploymentReady = databaseValidation.ok
    && runtimeValidation.ok
    && processorEnabled
    && buildReady
    && bindingsReady
    && accessConfigurationReady;
  const cohort = isInvoiceAsyncUserAllowed(env, {
    ...user,
    active: user.is_active ?? user.active,
    roles: [user.role, user.user_role, user.user_type]
  });
  const contractVersion = INVOICE_ASYNC_CONTRACT_VERSION;
  const featureFlags = Object.fromEntries(
    INVOICE_BATCH_MANDATORY_FEATURES.map(flag => [flag, deploymentReady])
  );
  return jsonResponse({
    contract_version: contractVersion,
    backend_contract_version: contractVersion,
    database_contract_ready: databaseValidation.ok,
    deployment_contract_ready: deploymentReady,
    pipeline_enabled: pipelineEnabled,
    processor_enabled: processorEnabled,
    enabled_for_user: pipelineEnabled && deploymentReady && cohort.allowed === true,
    enabled: pipelineEnabled && deploymentReady && cohort.allowed === true,
    access_mode: access.mode,
    controlled_cohort: access.mode === 'COHORT' && parsed.ok && parsed.ids.length > 0,
    scheduled_enabled: scheduledEnabled,
    supported_media_types: ['application/pdf','image/jpeg','image/png'],
    document_view_contract_version: INVOICE_DOCUMENT_ACCESS_CONTRACT,
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
  const requestErrors = new Set([
    'BATCH_COMMAND_TOKEN_INVALID',
    'BATCH_COMMAND_TOKEN_REQUIRED',
    'BATCH_CURSOR_INVALID',
    'BATCH_CURSOR_ISSUED_AT_INVALID',
    'BATCH_EXPLICIT_KEYS_INVALID',
    'BATCH_FACET_REQUEST_INVALID',
    'BATCH_FILTER_ARRAY_LIMIT_EXCEEDED',
    'BATCH_FILTER_BOOLEAN_INVALID',
    'BATCH_FILTER_DATE_RANGE_INVALID',
    'BATCH_FILTER_SEARCH_TOO_LONG',
    'BATCH_FILTER_STATUS_UNSUPPORTED',
    'BATCH_QUERY_ACTION_MISMATCH',
    'BATCH_QUERY_HASH_MISMATCH',
    'BATCH_QUERY_UNKNOWN_FIELD',
    'BATCH_REQUEST_JSON_INVALID',
    'BATCH_REQUEST_OBJECT_REQUIRED',
    'BATCH_SELECTION_CONTRACT_INVALID',
    'BATCH_SELECTION_DEFAULT_INVALID',
    'BATCH_SELECTION_INVALID',
    'BATCH_SELECTION_MODE_UNSUPPORTED',
    'BATCH_SELECTION_RULE_INVALID',
    'BATCH_SELECTION_RULE_LIMIT_EXCEEDED',
    'BATCH_SELECTION_RULE_SEQUENCE_DUPLICATE',
    'BATCH_SELECTION_RULE_SEQUENCE_INVALID',
    'BATCH_SELECTION_RULE_UNKNOWN_FIELD',
    'BATCH_SELECTION_RULES_INVALID',
    'BATCH_SELECTION_SELECTOR_INVALID',
    'BATCH_SELECTION_SELECTOR_UNKNOWN_FIELD',
    'BATCH_SELECTION_UNKNOWN_FIELD',
    'BATCH_SNAPSHOT_INVALID',
    'BATCH_SNAPSHOT_REQUIRED',
    'CANONICAL_COMMAND_REQUIRED',
    'CREDIT_NOTE_COMMAND_TOKEN_INVALID',
    'CREDIT_NOTE_COMMAND_TOKEN_REQUIRED',
    'CREDIT_NOTE_REASON_INVALID',
    'CREDIT_NOTE_REQUEST_INVALID',
    'DELIVERY_PART_NUMBER_INVALID',
    'DELIVERY_COMMAND_TOKEN_INVALID',
    'DELIVERY_POLICY_INVALID',
    'DELIVERY_REQUEST_INVALID',
    'DELIVERY_REQUEST_TOKEN_INVALID',
    'DELIVERY_REQUEST_TOKEN_REQUIRED',
    'DOCUMENT_PREPARATION_COMMAND_TOKEN_INVALID',
    'DOCUMENT_PREPARATION_COMMAND_TOKEN_REQUIRED',
    'DOCUMENT_PREPARATION_REQUEST_INVALID',
    'DOCUMENT_VERSION_ENTITY_TYPE_INVALID',
    'DOCUMENT_VERSION_ID_INVALID',
    'DOCUMENT_VERSION_POINTER_MISMATCH',
    'DOCUMENT_VERSION_PURPOSE_MISMATCH',
    'GENERATE_COMMAND_TOKEN_INVALID',
    'GENERATE_COMMAND_TOKEN_REQUIRED',
    'INVALID_ENTITY_ID',
    'INVALID_OUTBOX_QUEUE_STATE',
    'INVALID_OUTBOX_SEARCH',
    'INVALID_RECIPIENT_EMAIL',
    'INVOICE_ASYNC_DATABASE_CONTRACT_INVALID',
    'INVOICE_BATCH_DISPLAY_MODE_INVALID',
    'INVOICE_BATCH_FILTER_INVALID',
    'INVOICE_BATCH_FILTER_UNKNOWN_FIELD',
    'INVOICE_BATCH_GROUP_PRESET_INVALID',
    'INVOICE_BATCH_PAGE_SIZE_INVALID',
    'INVOICE_BATCH_QUERY_ACTION_MISMATCH',
    'INVOICE_BATCH_QUERY_CONTRACT_INVALID',
    'INVOICE_BATCH_QUERY_MODE_FIELD_INVALID',
    'INVOICE_BATCH_QUERY_MODE_INVALID',
    'INVOICE_BATCH_SORT_DIRECTION_INVALID',
    'INVOICE_BATCH_SORT_INVALID',
    'INVOICE_BATCH_SORT_KEY_INVALID',
    'INVOICE_ISSUE_REQUEST_INVALID',
    'INVOICE_START_RESULT_CORRELATION_INVALID',
    'ISSUE_COMMAND_TOKEN_INVALID',
    'ISSUE_COMMAND_TOKEN_REQUIRED',
    'ISSUE_DELIVERY_INTENT_INVALID',
    'ISSUE_DELIVERY_MODE_REQUIRED',
    'OPERATION_CONTROL_ACTION_INVALID',
    'OPERATION_CONTROL_ACTION_SCHEMA_INVALID',
    'OPERATION_CONTROL_REQUEST_HASH_MISMATCH',
    'OPERATION_CONTROL_REQUEST_TOKEN_INVALID',
    'OPERATION_CONTROL_REQUEST_TOKEN_REQUIRED',
    'OPERATION_RESULT_CATEGORY_INVALID',
    'OPERATION_RESULT_CURSOR_INVALID',
    'OPERATION_RESULT_PAGE_REQUEST_INVALID',
    'OPERATION_RESULT_ROOT_INVALID',
    'OUTBOX_CURSOR_INVALID',
    'READY_DOCUMENT_VERSION_NOT_FOUND',
    'RETRY_CHUNK_ID_INVALID',
    'VALID_UUID_ARRAY_REQUIRED'
  ]);
  const conflictErrors = new Set([
    'BATCH_CURSOR_ACTION_MISMATCH',
    'BATCH_CURSOR_EXPIRED',
    'BATCH_CURSOR_FILTER_MISMATCH',
    'BATCH_CURSOR_QUERY_MISMATCH',
    'BATCH_CURSOR_SNAPSHOT_MISMATCH',
    'BATCH_CURSOR_SORT_MISMATCH',
    'BATCH_SELECTION_EMPTY',
    'BATCH_SNAPSHOT_CHANGED',
    'BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION',
    'BATCH_SNAPSHOT_EXPIRED',
    'BATCH_SNAPSHOT_QUERY_MISMATCH',
    'BATCH_SOURCE_CHANGED',
    'DOCUMENT_PREPARATION_BLOCKED',
    'ISSUED_DOCUMENT_INTEGRITY_FAILURE',
    'ISSUED_DOCUMENT_POINTER_MISSING',
    'OPERATION_CONTROL_IDEMPOTENCY_CONFLICT',
    'OPERATION_CONTROL_IDEMPOTENCY_EXPIRED',
    'OPERATION_CONTROL_RECEIPT_IMMUTABLE',
    'OPERATION_RESULT_CURSOR_EXPIRED',
    'OPERATION_RESULT_CURSOR_STALE',
    'READY_DOCUMENT_IDENTITY_INVALID'
  ]);
  const unavailableErrors = new Set([
    'BATCH_SUMMARY_SCOPE_TOO_LARGE',
    'BATCH_SUMMARY_TIMEOUT',
    'INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH',
    'INVOICE_ASYNC_DATABASE_CONTRACT_UNAVAILABLE',
    'INVOICE_ASYNC_DATABASE_CONTRACT_MISMATCH',
    'INVOICE_ASYNC_DATABASE_COMPONENT_CONTRACT_MISMATCH',
    'INVOICE_ASYNC_DATABASE_NOT_READY',
    'INVOICE_ASYNC_DATABASE_PREREQUISITE_MISSING',
    'INVOICE_ASYNC_EXPECTED_MANIFEST_INVALID',
    'INVOICE_ASYNC_FUNCTION_MANIFEST_MISMATCH',
    'INVOICE_ASYNC_TEMPORARILY_UNAVAILABLE',
    'INVOICE_DOCUMENT_ACCESS_SECRET_MISSING',
    'BATCH_CURSOR_SECRET_INVALID',
    'OUTBOX_CURSOR_SECRET_INVALID',
    'UNIFIED_OUTBOX_LIST_FAILED'
  ]);
  if (requestErrors.has(code)) return 400;
  if (conflictErrors.has(code)) return 409;
  if (unavailableErrors.has(code)) return 503;
  if (['BATCH_REQUEST_TOO_LARGE', 'BATCH_SELECTION_PAYLOAD_TOO_LARGE'].includes(code)) return 413;
  if (code === 'INVOICE_LEGACY_ROUTE_RETIRED') return 410;
  if (['INVOICE_ASYNC_BACKPRESSURE', 'INVOICE_ASYNC_RATE_LIMITED'].includes(code)) return 429;
  // Compatibility mapping for non-V8 authentication/authorisation failures only.
  if (/UNAUTHENTICATED|SESSION/.test(code)) return 401;
  if (/FORBIDDEN|ADMIN_REQUIRED|PERMISSION/.test(code)) return 403;
  return 500;
}

function isRetiredInvoiceLegacyRoute(req, url) {
  const path = url.pathname;
  return (
    (req.method === 'POST' && [
      '/api/invoices',
      '/api/invoices/tsfin/by-week',
      '/api/invoices/create-expenses',
      '/api/nhsp/invoices/run',
      '/api/invpdf/queue/drain',
      '/api/tspdf/queue/drain'
    ].includes(path))
    || ['/internal/invpdf-worker', '/api/invpdf/worker'].includes(path)
  );
}

function isInvoiceAsyncRoute(req, url) {
  const path = url.pathname;
  const method = req.method;
  if (method === 'GET' && path === '/api/invoice-documents/access') return true;
  if (method === 'GET' && path === '/api/invoice-async/capabilities') return true;
  if (['GET', 'POST'].includes(method) && [
    '/api/invoices/batch-generate/candidates',
    '/api/invoices/batch-issue/candidates',
    '/api/nhsp/invoices/candidates'
  ].includes(path)) return true;
  if (method === 'POST' && [
    '/api/invoices/batch-generate/confirm',
    '/api/invoices/batch-issue/confirm',
    '/api/invoice-operations/get',
    '/api/invoice-operations/control'
  ].includes(path)) return true;
  if (isRetiredInvoiceLegacyRoute(req, url)) return true;
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
  if (!isInvoiceAsyncRoute(req, url)) return null;
  if (isRetiredInvoiceLegacyRoute(req, url)) {
    return jsonResponse({
      ok: false,
      contract_version: INVOICE_ASYNC_CONTRACT_VERSION,
      error: {
        code: 'INVOICE_LEGACY_ROUTE_RETIRED',
        message: 'This invoice action has moved to the asynchronous invoice workflow.',
        retryable: false
      }
    }, 410);
  }
  if (req.method === 'GET' && [
    '/api/invoices/batch-generate/candidates',
    '/api/invoices/batch-issue/candidates',
    '/api/nhsp/invoices/candidates'
  ].includes(path)) {
    return jsonResponse({ ok: false, error: 'BATCH_QUERY_POST_REQUIRED' }, 405, { allow: 'POST' });
  }
  if (!isInvoiceAsyncPipelineEnabled(env)) {
    return jsonResponse({
      ok: false,
      contract_version: INVOICE_ASYNC_CONTRACT_VERSION,
      error: 'INVOICE_ASYNC_TEMPORARILY_UNAVAILABLE'
    }, 503);
  }

  const user = await requireActor(env, req, deps, false);
  if (!user) return jsonResponse({ error: 'UNAUTHENTICATED' }, 401);
  const cohort = isInvoiceAsyncUserAllowed(env, {
    ...user,
    active: user.is_active ?? user.active,
    roles: [user.role, user.user_role, user.user_type]
  });
  if (!cohort.allowed) {
    if (cohort.code === 'INVOICE_ASYNC_ALLOWLIST_INVALID'
        || cohort.code === 'INVOICE_ASYNC_ACCESS_MODE_INVALID') {
      return jsonResponse({ error: cohort.code }, 503);
    }
    return jsonResponse({ error: cohort.code }, 403);
  }

  const databaseContract = await loadInvoiceAsyncDatabaseContract(env, deps);
  const databaseValidation = validateInvoiceAsyncDatabaseContract(env, databaseContract);
  const runtimeValidation = validateQueueRuntimeConfiguration(env);
  const processorReady = await invoiceProcessorReady(env);
  if (!databaseValidation.ok
      || !runtimeValidation.ok
      || !processorReady
      || !String(env.INVOICE_ASYNC_BUILD_ID || '').trim()
      || !env.INVOICE_QUEUE_DISPATCHER
      || !env.R2
      || !cursorSecret(env, 'CANDIDATE')
      || !cursorSecret(env, 'RESULT')
      || !env.INVOICE_DOCUMENT_ACCESS_SECRET) {
    return jsonResponse({
      ok: false,
      contract_version: INVOICE_ASYNC_CONTRACT_VERSION,
      error: 'INVOICE_ASYNC_TEMPORARILY_UNAVAILABLE'
    }, 503);
  }

  try {
    if (req.method === 'POST' && path === '/api/invoices/batch-generate/candidates') {
      return await handleCandidates(env, req, deps, 'invoice_batch_generate_candidates');
    }
    if (req.method === 'POST' && path === '/api/invoices/batch-issue/candidates') {
      return await handleCandidates(env, req, deps, 'invoice_batch_issue_candidates');
    }
    if (req.method === 'POST' && path === '/api/nhsp/invoices/candidates') {
      return await handleNhspCandidates(env, req, deps);
    }
    if (req.method === 'POST' && path === '/api/invoices/batch-generate/confirm') {
      return await handleBatchGenerateConfirm(env, req, ctx, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoices/batch-issue/confirm') {
      return await handleBatchIssueConfirm(env, req, ctx, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoice-operations/get') {
      return await handleOperationGet(env, req, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoice-operations/control') {
      return await handleOperationControl(env, req, ctx, user, deps);
    }
    if (req.method === 'GET' && path === '/api/outbox') {
      const channel = String(url.searchParams.get('channel') || '').trim().toUpperCase();
      if (!channel || channel === 'INVOICE') {
        return await handleInvoiceOutboxList(env, req, deps);
      }
    }

    let params = match(path, '/api/invoice-operations/:operation_id');
    if (params && req.method === 'GET') {
      return await handleOperationGet(env, req, user, deps, params.operation_id);
    }
    params = match(path, '/api/invoice-document-versions/:document_version_id/presign');
    if (params && req.method === 'POST') {
      return await handleReadyInvoiceDocumentPresign(
        env,
        user,
        params.document_version_id
      );
    }
    params = match(path, '/api/invoice-document-versions/:document_version_id/download');
    if (params && req.method === 'GET') {
      return await handleReadyInvoiceDocumentDownload(
        env,
        req,
        user,
        params.document_version_id
      );
    }
    params = match(path, '/api/outbox/:channel/:operation_id');
    if (params && String(params.channel).toUpperCase() === 'INVOICE') {
      if (req.method === 'GET') {
        return await handleOperationGet(env, req, user, deps, params.operation_id);
      }
      if (req.method === 'DELETE') {
        return await handleInvoiceOutboxControl(
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
      return await handleInvoiceOutboxControl(
        env, req, ctx, user, deps, params.operation_id, 'RETRY'
      );
    }
    params = match(path, '/api/outbox/:channel/:operation_id/reschedule');
    if (
      params
      && req.method === 'POST'
      && String(params.channel).toUpperCase() === 'INVOICE'
    ) {
      return await handleInvoiceOutboxControl(
        env, req, ctx, user, deps, params.operation_id, 'RESCHEDULE'
      );
    }
    params = match(path, '/api/invoices/:invoice_id/render');
    if (params && req.method === 'POST') {
      return await handleViewDocument(env, req, ctx, user, deps, 'INVOICE', params.invoice_id);
    }
    params = match(path, '/api/timesheets/:timesheet_id/pdf');
    if (params && req.method === 'GET') {
      return jsonResponse({
        ok: false,
        error: 'DOCUMENT_PREPARATION_POST_REQUIRED'
      }, 405, { allow: 'POST' });
    }
    if (params && req.method === 'POST') {
      return await handleViewDocument(env, req, ctx, user, deps, 'TIMESHEET', params.timesheet_id);
    }
    params = match(path, '/api/invoices/:invoice_id/issue');
    if (params && req.method === 'POST') {
      return await handleIssueOne(env, req, ctx, user, deps, params.invoice_id);
    }
    params = match(path, '/api/invoices/:invoice_id/email');
    if (params && req.method === 'POST') {
      return await handleDeliverOne(env, req, ctx, user, deps, params.invoice_id);
    }
    params = match(path, '/api/invoices/:invoice_id/credit-note');
    if (params && req.method === 'POST') {
      const body = await readInvoiceBatchJsonBody(req, {
        maximumBytes: Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES)
          || INVOICE_BATCH_REQUEST_MAX_BYTES
      });
      const allowedFields = new Set(['credit_reason', 'command_token']);
      if (Object.keys(body).some(key => !allowedFields.has(key))) {
        throw invoiceBatchContractError('CREDIT_NOTE_REQUEST_INVALID');
      }
      const creditReason = String(body.credit_reason || '').trim();
      if (!creditReason || creditReason.length > 1000
          || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(creditReason)) {
        throw invoiceBatchContractError('CREDIT_NOTE_REASON_INVALID');
      }
      return await startCommands(env, req, ctx, user, [{
        command_type: 'GENERATE_CREDIT_NOTE',
        base_invoice_id: params.invoice_id,
        credit_reason: creditReason,
        command_token: commandToken(req, body, {
          requiredCode: 'CREDIT_NOTE_COMMAND_TOKEN_REQUIRED',
          invalidCode: 'CREDIT_NOTE_COMMAND_TOKEN_INVALID'
        })
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
  commandToken,
  normaliseDeliveryRequestToken,
  generationCommandFromBody,
  readInvoiceBatchJsonBody,
  validateInvoiceBatchSourceFields,
  normaliseInvoiceBatchFilters,
  normaliseInvoiceBatchSort,
  normaliseInvoiceBatchSelectionRules,
  normaliseInvoiceBatchSnapshot,
  normaliseInvoiceBatchFacetRequest,
  normaliseInvoiceBatchExplicitKeys,
  normaliseInvoiceBatchCursorValues,
  normaliseInvoiceBatchQueryBody,
  hashInvoiceBatchFilter,
  hashInvoiceBatchQuery,
  hashInvoiceBatchSelection,
  encodeInvoiceBatchCursor,
  decodeInvoiceBatchCursor,
  encodeInvoiceBatchResultCursorV2,
  decodeInvoiceBatchResultCursorV2,
  candidateGroupsFromRpc,
  normaliseInvoiceOperationControlAction,
  invoiceOperationControlEnvelope,
  loadInvoiceAsyncDatabaseContract,
  validateInvoiceAsyncDatabaseContract,
  invoiceErrorStatus,
  isInvoiceAsyncRoute,
  isRetiredInvoiceLegacyRoute,
  encodeUnifiedOutboxCursor,
  decodeUnifiedOutboxCursor,
  compareCursorOutboxRows,
  legacyQueueState,
  match
});
