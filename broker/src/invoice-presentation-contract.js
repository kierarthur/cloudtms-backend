const OBJECT = value => value && typeof value === 'object' && !Array.isArray(value);
const HEX_64 = /^[0-9a-f]{64}$/;

function contractError(code, detail) {
  return Object.assign(new Error(code), { code, detail });
}

function requireObject(value, code = 'RENDER_MODEL_INVALID') {
  if (!OBJECT(value)) throw contractError(code);
  return value;
}

function requireArray(value, code = 'RENDER_MODEL_INVALID') {
  if (!Array.isArray(value)) throw contractError(code);
  return value;
}

function requireText(value, code = 'RENDER_MODEL_REQUIRED_FIELD_MISSING') {
  if (!String(value ?? '').trim()) throw contractError(code);
  return String(value);
}

function rejectMutableUrls(value, path = '$') {
  if (Array.isArray(value)) {
    value.forEach((entry, index) => rejectMutableUrls(entry, `${path}[${index}]`));
    return;
  }
  if (!OBJECT(value)) return;
  for (const [key, entry] of Object.entries(value)) {
    if (/(^|_)url$/i.test(key) && typeof entry === 'string' && /^https?:/i.test(entry)) {
      throw contractError('RENDER_MODEL_MUTABLE_URL_FORBIDDEN', `${path}.${key}`);
    }
    rejectMutableUrls(entry, `${path}.${key}`);
  }
}

function validateAssetIdentity(identity) {
  if (identity == null) return;
  requireObject(identity, 'RENDER_ASSET_IDENTITY_INVALID');
  if (!identity.r2_key) {
    if (Object.values(identity).some(value => value != null && value !== '')) {
      throw contractError('RENDER_ASSET_IDENTITY_INVALID');
    }
    return;
  }
  requireText(identity.r2_key, 'RENDER_ASSET_IDENTITY_INVALID');
  if (!HEX_64.test(String(identity.sha256 || '').toLowerCase())) {
    throw contractError('RENDER_ASSET_HASH_REQUIRED');
  }
  if (identity.size_bytes != null && (!Number.isSafeInteger(Number(identity.size_bytes)) || Number(identity.size_bytes) < 1)) {
    throw contractError('RENDER_ASSET_IDENTITY_INVALID');
  }
  if (!['image/jpeg', 'image/png'].includes(String(identity.media_type || '').toLowerCase())) {
    throw contractError('RENDER_ASSET_MEDIA_UNSUPPORTED');
  }
}

function validateRowKeys(rows) {
  const keys = new Set();
  for (const row of rows) {
    requireObject(row);
    const key = requireText(row.row_key);
    if (keys.has(key)) throw contractError('RENDER_MODEL_ROW_KEY_DUPLICATE');
    keys.add(key);
  }
}

export function validateFrozenInvoicePresentationModel(model, options = {}) {
  requireObject(model);
  if (model.schema_version !== 'INVOICE_RENDER_MODEL_V1') {
    throw contractError('RENDER_MODEL_SCHEMA_UNSUPPORTED');
  }
  requireText(model.purpose);
  requireText(model.document_type);
  requireObject(model.supplier);
  requireObject(model.customer);
  requireText(model.supplier.legal_name);
  requireText(model.customer.legal_name);
  requireObject(model.references);
  requireObject(model.totals);
  requireObject(model.payment);
  requireObject(model.credit_note);
  requireObject(model.self_bill);
  requireArray(model.vat_breakdown);
  requireArray(model.legal_wording);
  const lines = requireArray(model.lines);
  if (lines.length === 0 && Number(model.totals.net || 0) !== 0) {
    throw contractError('INVOICE_PRESENTATION_MODEL_INVALID');
  }
  validateRowKeys(lines);
  for (const line of lines) {
    for (const field of ['source_invoice_line_id', 'description', 'unit', 'quantity', 'unit_price', 'net_amount', 'vat_rate', 'vat_amount', 'gross_amount', 'display_order']) {
      if (line[field] == null || line[field] === '') throw contractError('INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING', field);
    }
  }
  if (model.document_type === 'CREDIT_NOTE') {
    requireText(model.credit_note.original_invoice_number, 'INVOICE_CREDIT_NOTE_RELATIONSHIP_MISSING');
    requireText(model.credit_note.reason, 'INVOICE_CREDIT_NOTE_REASON_MISSING');
  }
  if (model.document_type === 'SELF_BILL_INVOICE') {
    requireText(model.self_bill.legal_wording, 'INVOICE_SELF_BILL_WORDING_MISSING');
  }
  if (options.templateVersion && model.template_version !== options.templateVersion) {
    throw contractError('RENDER_TEMPLATE_VERSION_MISMATCH');
  }
  validateAssetIdentity(model.branding?.logo);
  rejectMutableUrls(model);
  return model;
}

export function validateFrozenTimesheetPresentationModel(model, options = {}) {
  requireObject(model);
  if (model.schema_version !== 'TIMESHEET_RENDER_MODEL_V1') {
    throw contractError('RENDER_MODEL_SCHEMA_UNSUPPORTED');
  }
  requireText(model.timesheet_id);
  requireObject(model.candidate);
  requireObject(model.client);
  requireObject(model.contract);
  requireObject(model.work);
  requireObject(model.references);
  requireObject(model.authorisation);
  requireObject(model.signatures);
  requireObject(model.qr);
  requireArray(model.daily_schedule_rows);
  requireArray(model.weekly_schedule_rows);
  for (const row of [...model.daily_schedule_rows, ...model.weekly_schedule_rows]) {
    requireObject(row);
    if (!Number.isSafeInteger(Number(row.display_order)) || Number(row.display_order) < 1) {
      throw contractError('TIMESHEET_PRESENTATION_MODEL_INVALID');
    }
  }
  validateAssetIdentity(model.signatures.candidate);
  validateAssetIdentity(model.signatures.authoriser);
  if (options.templateVersion && model.template_version !== options.templateVersion) {
    throw contractError('RENDER_TEMPLATE_VERSION_MISMATCH');
  }
  rejectMutableUrls(model);
  return model;
}

function validateSupportModel(model, schemaVersion, fields) {
  requireObject(model);
  if (model.schema_version !== schemaVersion) throw contractError('RENDER_MODEL_SCHEMA_UNSUPPORTED');
  for (const row of requireArray(model.rows)) {
    requireObject(row);
    for (const field of fields) {
      if (!Object.prototype.hasOwnProperty.call(row, field)) {
        throw contractError('SUPPORT_PRESENTATION_MODEL_INVALID', field);
      }
    }
  }
  rejectMutableUrls(model);
  return model;
}

export function validateFrozenHealthRosterModel(model) {
  return validateSupportModel(model, 'HEALTHROSTER_PRESENTATION_V1', [
    'worker', 'assignment', 'shift_date', 'shift_times', 'site', 'ward',
    'reference', 'units_hours', 'validation_state', 'source_identity'
  ]);
}

export function validateFrozenNhspModel(model) {
  return validateSupportModel(model, 'NHSP_PRESENTATION_V1', [
    'worker', 'nhsp_shift_id', 'booking_reference', 'site_ward', 'shift_date',
    'shift_times', 'hours_units', 'source_identity', 'validation_state'
  ]);
}

export function validateFrozenHigherRateModel(model) {
  return validateSupportModel(model, 'HIGHER_RATE_PRESENTATION_V1', [
    'worker_source', 'shift_date', 'original_rate', 'applied_rate', 'units',
    'display_amount', 'reason', 'approval_identity', 'reference'
  ]);
}

export function validateFrozenAttachmentIndexModel(model) {
  requireObject(model);
  const rows = requireArray(model.display_rows);
  const ids = new Set();
  let previousStart = 0;
  for (const row of rows) {
    const id = requireText(row.row_id, 'ATTACHMENT_INDEX_ROW_ID_MISSING');
    if (ids.has(id)) throw contractError('ATTACHMENT_INDEX_ROW_DUPLICATE');
    ids.add(id);
    const start = Number(row.start_page);
    const pages = Number(row.page_count);
    if (!Number.isSafeInteger(start) || start < 1 || start < previousStart) {
      throw contractError('ATTACHMENT_INDEX_START_PAGE_INVALID');
    }
    if (!Number.isSafeInteger(pages) || pages < 1) {
      throw contractError('ATTACHMENT_INDEX_PAGE_COUNT_INVALID');
    }
    previousStart = start;
  }
  return model;
}

export function validateFrozenPresentationModel(renderKind, model, options = {}) {
  const kind = String(renderKind || '').toUpperCase();
  if (kind === 'INVOICE_CORE') return validateFrozenInvoicePresentationModel(model, options);
  if (kind === 'ELECTRONIC_TIMESHEET') return validateFrozenTimesheetPresentationModel(model, options);
  if (kind === 'HEALTHROSTER_SUPPORT') return validateFrozenHealthRosterModel(model);
  if (kind === 'NHSP_SUPPORT') return validateFrozenNhspModel(model);
  if (kind === 'HIGHER_RATE_SUPPORT') return validateFrozenHigherRateModel(model);
  if (kind === 'ATTACHMENT_INDEX') return validateFrozenAttachmentIndexModel(model);
  if (kind === 'SECTION_SEPARATOR') return requireObject(model);
  throw contractError('RENDER_MODEL_KIND_MISMATCH');
}
