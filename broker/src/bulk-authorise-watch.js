const VOLATILE_KEYS = new Set([
  'watch_vector',
  'signed_url',
  'signedUrl',
  'preview_url',
  'previewUrl',
  'download_url',
  'downloadUrl',
  'request_id',
  'requestId',
  'trace_id',
  'traceId',
  'server_now',
  'serverNow'
]);

// Only fields deliberately present in both the full context and canonical
// active-row header belong here. Full profiles add editor/schedule objects;
// hashing the entire row would therefore invalidate every unchanged revisit.
// The authoritative backend signature below catches timesheet, contract-week
// and financial row changes. This projection adds the user-visible routing,
// policy, permission, validation and display state that can also be affected
// by related contract/client records.
const HEADER_WATCH_FIELDS = Object.freeze([
  'row_key',
  'timesheet_id',
  'current_timesheet_id',
  'contract_week_id',
  'contract_id',
  'candidate_id',
  'candidate_name',
  'candidate_display_name',
  'client_id',
  'client_name',
  'booking_id',
  'week_ending_date',
  'period_type',
  'sheet_scope',
  'submission_mode',
  'submission_mode_snapshot',
  'basis',
  'route_family',
  'summary_stage',
  'tools_stage',
  'processing_status',
  'authorised_at_utc',
  'authorised_at_server',
  'processed_at_utc',
  'is_authorised',
  'locked',
  'has_retained_financial_history',
  'can_unprocess',
  'unprocess_action_visible',
  'unprocess_block_reason',
  'unprocess_block_message',
  'total_hours',
  'total_pay_ex_vat',
  'total_charge_ex_vat',
  'margin_ex_vat',
  'paid_at_utc',
  'invoice_is_paid',
  'invoice_segments_locked',
  'issue_codes',
  'validation_status',
  'hr_crosscheck_status',
  'hr_crosscheck_issues',
  'qr_status',
  'is_qr',
  'is_adjusted',
  'needs_attention',
  'has_rate_issue',
  'has_pay_channel_issue',
  'client_no_timesheet_required',
  'client_autoprocess_hr',
  'client_is_nhsp',
  'is_adjustment',
  'additional_seq',
  'suppress_standard_schedule_fallback',
  'keep_additional_manual_adjustment_schedule_empty'
]);

function canonicalEvidenceRow(item) {
  const source = item && typeof item === 'object' ? item : {};
  return cleanForWatch({
    id: text(source.id || source.evidence_id) || null,
    kind: text(source.kind || source.evidence_kind || source.type).toUpperCase() || null,
    storage_key: text(source.storage_key || source.r2_key || source.file_key || source.download_storage_key) || null,
    display_name: text(source.display_name || source.filename || source.original_filename || source.file_name) || null,
    created_at: source.created_at || source.uploaded_at_utc || null,
    rotation_degrees: source.rotation_degrees ?? source.rotation ?? source.last_rotation_deg ?? 0,
    timesheet_id: text(source.timesheet_id) || null,
    system: source.system === true,
    is_view_only: source.is_view_only === true
  });
}

function cleanForWatch(value) {
  if (Array.isArray(value)) return value.map(cleanForWatch);
  if (!value || typeof value !== 'object') return value;
  const out = {};
  for (const key of Object.keys(value).sort()) {
    if (VOLATILE_KEYS.has(key) || key.startsWith('__')) continue;
    const next = cleanForWatch(value[key]);
    if (next !== undefined) out[key] = next;
  }
  return out;
}

function stableStringify(value) {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

async function hashValue(value) {
  const input = new TextEncoder().encode(stableStringify(cleanForWatch(value)));
  const digest = await crypto.subtle.digest('SHA-256', input);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function text(value) {
  return String(value == null ? '' : value).trim();
}

function evidenceRowsFromContext(payload) {
  const source = payload && typeof payload === 'object' ? payload : {};
  const rows = Array.isArray(source.evidence)
    ? source.evidence
    : (Array.isArray(source.attached_evidence)
        ? source.attached_evidence
        : (Array.isArray(source.details?.evidence) ? source.details.evidence : []));
  return rows
    .map(canonicalEvidenceRow)
    .sort((left, right) => {
      const leftKey = [text(left.id || left.evidence_id), text(left.storage_key || left.r2_key), text(left.kind), text(left.created_at)].join('|');
      const rightKey = [text(right.id || right.evidence_id), text(right.storage_key || right.r2_key), text(right.kind), text(right.created_at)].join('|');
      return leftKey.localeCompare(rightKey);
    });
}

function importPayloadFromContext(payload) {
  const source = payload && typeof payload === 'object' ? payload : {};
  const details = source.details && typeof source.details === 'object' ? source.details : {};
  const leftPane = source.left_pane && typeof source.left_pane === 'object' ? source.left_pane : {};
  const compare = source.compare_payload && typeof source.compare_payload === 'object'
    ? source.compare_payload
    : (source.compare && typeof source.compare === 'object'
        ? source.compare
        : (details.compare_payload && typeof details.compare_payload === 'object'
            ? details.compare_payload
            : (details.healthroster_compare && typeof details.healthroster_compare === 'object' ? details.healthroster_compare : {})));
  // The full and compare_import profiles wrap the same comparison rows under
  // different keys. Hash the canonical row set, not profile-only wrapper
  // metadata such as `required` or `include_import_source_rows`. The backend
  // row signature independently covers the current timesheet/financial rows.
  const candidates = [
    compare.rows,
    compare.source_rows,
    compare.external_source_rows_json,
    details.source_rows,
    details.external_source_rows_json,
    leftPane.source_items
  ];
  const rows = candidates.find((value) => Array.isArray(value)) || [];
  return rows
    .map(cleanForWatch)
    .sort((left, right) => stableStringify(left).localeCompare(stableStringify(right)));
}

function rowFromContext(payload) {
  const source = payload && typeof payload === 'object' ? payload : {};
  return cleanForWatch(
    (source.data_row && typeof source.data_row === 'object' ? source.data_row : null) ||
    (source.row && typeof source.row === 'object' ? source.row : null) ||
    {}
  );
}

function headerWatchRow(payload) {
  const row = rowFromContext(payload);
  const out = {};
  for (const key of HEADER_WATCH_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(row, key)) out[key] = row[key];
  }
  return cleanForWatch(out);
}

function backendSignatureFromContext(payload, row) {
  const source = payload && typeof payload === 'object' ? payload : {};
  const dataRow = row && typeof row === 'object' ? row : {};
  return text(
    source.backend_row_signature ||
    source.row_backend_signature ||
    source.row_signature ||
    dataRow.backend_row_signature ||
    dataRow.row_backend_signature ||
    dataRow.row_signature
  );
}

export async function buildBulkAuthoriseWatchVector(payload, options = {}) {
  const includeEvidence = options.includeEvidence === true;
  const includeImport = options.includeImport === true;
  const row = rowFromContext(payload);
  const coreToken = backendSignatureFromContext(payload, row);
  const headerToken = await hashValue(headerWatchRow(payload));
  const evidenceToken = includeEvidence ? await hashValue(evidenceRowsFromContext(payload)) : '';
  const importToken = includeImport ? await hashValue(importPayloadFromContext(payload)) : '';
  const domainTokens = {
    core: coreToken,
    header: headerToken,
    evidence: evidenceToken,
    import_source: importToken
  };
  const cacheable = !!(
    coreToken &&
    headerToken &&
    (!includeEvidence || evidenceToken) &&
    (!includeImport || importToken)
  );
  return {
    version: 1,
    cacheable,
    include_evidence: includeEvidence,
    include_import: includeImport,
    row_key: text(payload?.row_key || row.row_key) || null,
    timesheet_id: text(payload?.current_timesheet_id || payload?.timesheet_id || row.current_timesheet_id || row.timesheet_id) || null,
    contract_week_id: text(payload?.contract_week_id || row.contract_week_id) || null,
    domain_tokens: domainTokens,
    watch_token: cacheable ? await hashValue(domainTokens) : ''
  };
}

export function watchVectorMatches(left, right) {
  const a = left && typeof left === 'object' ? left : {};
  const b = right && typeof right === 'object' ? right : {};
  return !!(
    a.cacheable === true &&
    b.cacheable === true &&
    text(a.watch_token) &&
    text(a.watch_token) === text(b.watch_token)
  );
}

export const __bulkAuthoriseWatchTest = {
  cleanForWatch,
  stableStringify,
  evidenceRowsFromContext,
  importPayloadFromContext,
  rowFromContext,
  headerWatchRow,
  canonicalEvidenceRow
};
