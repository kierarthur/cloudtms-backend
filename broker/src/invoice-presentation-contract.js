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
    if (['sha256', 'size_bytes', 'media_type', 'data_url']
      .some(field => identity[field] != null && identity[field] !== '')) {
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
  requireObject(model, 'RENDER_MODEL_INVALID');

  const fail = (code, detail) => { throw contractError(code, detail); };
  const asNumber = (value, code, detail) => {
    const number = Number(value);
    if (!Number.isFinite(number)) fail(code, detail);
    return number;
  };
  const assertFiniteMoney = (value, detail) => asNumber(value, 'INVOICE_PRESENTATION_NUMERIC_INVALID', detail);
  const near = (left, right, tolerance = 0.01) =>
    Math.abs(Number(left || 0) - Number(right || 0)) <= tolerance;
  const requireTextArray = (value, code, detail) => {
    if (!Array.isArray(value)) fail(code, detail);
    const out = value
      .map(entry => String(entry ?? '').trim())
      .filter(Boolean);
    if (!out.length) fail(code, detail);
    return out;
  };
  const requireOptionalTextArray = (value, code, detail) => {
    if (value == null) return [];
    if (!Array.isArray(value)) fail(code, detail);
    return value.map(entry => String(entry ?? '').trim()).filter(Boolean);
  };
  const forbiddenPaySideKeys = new Set([
    'pay_day',
    'pay_night',
    'pay_sat',
    'pay_sun',
    'pay_bh',
    'total_pay_ex_vat',
    'pay_total_inc_vat_snapshot',
    'pay_vat_amount_snapshot',
    'pay_vat_rate_pct_snapshot',
    'margin_ex_vat',
    'policy_snapshot_json',
    'rate_source_refs_json',
    'candidate_pay_rate',
    'pay_rate',
    'pay_method'
  ]);
  const assertNoPaySideFields = (value, path = '$') => {
    if (Array.isArray(value)) {
      value.forEach((entry, index) => assertNoPaySideFields(entry, `${path}[${index}]`));
      return;
    }
    if (!value || typeof value !== 'object') return;
    for (const [key, entry] of Object.entries(value)) {
      if (forbiddenPaySideKeys.has(key)) fail('INVOICE_PRESENTATION_PAY_SIDE_FIELD_FORBIDDEN', `${path}.${key}`);
      assertNoPaySideFields(entry, `${path}.${key}`);
    }
  };

  if (model.schema_version !== 'INVOICE_RENDER_MODEL_V1') {
    fail('RENDER_MODEL_SCHEMA_UNSUPPORTED', model.schema_version);
  }

  const purpose = requireText(model.purpose, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  const documentType = requireText(model.document_type, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  if (!['INVOICE', 'SELF_BILL_INVOICE', 'CREDIT_NOTE'].includes(documentType)) {
    fail('INVOICE_PRESENTATION_DOCUMENT_TYPE_UNSUPPORTED', documentType);
  }

  requireObject(model.supplier, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.customer, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.references, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.totals, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.payment, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.credit_note, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.self_bill, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');

  requireText(model.supplier.legal_name, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireTextArray(model.supplier.registered_address, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING', 'supplier.registered_address');
  requireText(model.customer.legal_name, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireTextArray(model.customer.billing_address, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING', 'customer.billing_address');
  requireText(model.payment.terms_text, 'INVOICE_PRESENTATION_PAYMENT_TERMS_REQUIRED');

  if (purpose === 'FINAL_ISSUE') {
    requireText(model.invoice_number, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
    requireText(model.issue_date, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
    requireText(model.tax_point, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
    requireText(model.due_date, 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING');
  }

  if (options.templateVersion && model.template_version !== options.templateVersion) {
    fail('RENDER_TEMPLATE_VERSION_MISMATCH', { expected: options.templateVersion, actual: model.template_version });
  }

  const currency = String(model.currency || 'GBP');
  if (!currency.trim()) fail('INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING', 'currency');

  const lines = requireArray(model.lines, 'INVOICE_PRESENTATION_MODEL_INVALID');
  validateRowKeys(lines);

  const seenDisplayOrders = new Set();
  let lineNetTotal = 0;
  let lineVatTotal = 0;
  let lineGrossTotal = 0;

  for (const line of lines) {
    requireObject(line, 'INVOICE_PRESENTATION_LINE_INVALID');

    for (const field of [
      'row_key',
      'source_invoice_line_id',
      'source_key',
      'description',
      'unit',
      'quantity',
      'unit_price',
      'net_amount',
      'vat_rate',
      'vat_amount',
      'gross_amount',
      'display_order'
    ]) {
      if (line[field] == null || line[field] === '') {
        fail('INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING', `lines[].${field}`);
      }
    }

    const displayOrder = Number(line.display_order);
    if (!Number.isSafeInteger(displayOrder) || displayOrder < 1) {
      fail('INVOICE_PRESENTATION_DISPLAY_ORDER_INVALID', line.row_key);
    }
    if (seenDisplayOrders.has(displayOrder)) {
      fail('INVOICE_PRESENTATION_DISPLAY_ORDER_DUPLICATE', displayOrder);
    }
    seenDisplayOrders.add(displayOrder);

    const quantity = asNumber(line.quantity, 'INVOICE_PRESENTATION_NUMERIC_INVALID', `${line.row_key}.quantity`);
    const unitPrice = asNumber(line.unit_price, 'INVOICE_PRESENTATION_NUMERIC_INVALID', `${line.row_key}.unit_price`);
    const net = assertFiniteMoney(line.net_amount, `${line.row_key}.net_amount`);
    const vat = assertFiniteMoney(line.vat_amount, `${line.row_key}.vat_amount`);
    const gross = assertFiniteMoney(line.gross_amount, `${line.row_key}.gross_amount`);
    asNumber(line.vat_rate, 'INVOICE_PRESENTATION_NUMERIC_INVALID', `${line.row_key}.vat_rate`);

    if (line.reference_required === true && !String(line.reference || '').trim()) {
      fail('INVOICE_PRESENTATION_REFERENCE_REQUIRED', line.row_key);
    }
    if (line.reference_scope != null && !String(line.reference_scope).trim()) {
      fail('INVOICE_PRESENTATION_REFERENCE_IDENTITY_INVALID', `${line.row_key}.reference_scope`);
    }
    if (line.reference_source != null && !String(line.reference_source).trim()) {
      fail('INVOICE_PRESENTATION_REFERENCE_IDENTITY_INVALID', `${line.row_key}.reference_source`);
    }
    if (line.reference_source_row_keys != null) {
      if (!Array.isArray(line.reference_source_row_keys)
        || line.reference_source_row_keys.some(value => !String(value || '').trim())) {
        fail('INVOICE_PRESENTATION_REFERENCE_IDENTITY_INVALID', `${line.row_key}.reference_source_row_keys`);
      }
    }

    if (!near(net + vat, gross)) {
      fail('INVOICE_PRESENTATION_LINE_TOTAL_MISMATCH', line.row_key);
    }

    // A zero quantity/unit price is legitimate for some corrections and descriptive credit rows;
    // the hard rule is finite numeric identity, not non-zero economics.
    if (!Number.isFinite(quantity) || !Number.isFinite(unitPrice)) {
      fail('INVOICE_PRESENTATION_NUMERIC_INVALID', line.row_key);
    }

    lineNetTotal += net;
    lineVatTotal += vat;
    lineGrossTotal += gross;
  }

  const totals = model.totals;
  const totalNet = assertFiniteMoney(totals.net, 'totals.net');
  const totalVat = assertFiniteMoney(totals.vat, 'totals.vat');
  const totalGross = assertFiniteMoney(totals.gross, 'totals.gross');
  assertFiniteMoney(totals.amount_paid ?? 0, 'totals.amount_paid');
  assertFiniteMoney(totals.amount_credited ?? 0, 'totals.amount_credited');
  assertFiniteMoney(totals.amount_outstanding ?? totalGross, 'totals.amount_outstanding');

  if (lines.length === 0 && !near(totalNet, 0)) {
    fail('INVOICE_PRESENTATION_MODEL_INVALID', 'non_zero_invoice_has_no_lines');
  }
  if (!near(lineNetTotal, totalNet) || !near(lineVatTotal, totalVat) || !near(lineGrossTotal, totalGross)) {
    fail('INVOICE_PRESENTATION_LINE_TOTAL_MISMATCH', {
      line_net: lineNetTotal,
      line_vat: lineVatTotal,
      line_gross: lineGrossTotal,
      total_net: totalNet,
      total_vat: totalVat,
      total_gross: totalGross
    });
  }
  if (!near(totalNet + totalVat, totalGross)) {
    fail('INVOICE_PRESENTATION_TOTAL_MISMATCH', 'net_plus_vat_must_equal_gross');
  }

  const vatBreakdown = requireArray(model.vat_breakdown, 'INVOICE_PRESENTATION_MODEL_INVALID');
  if (vatBreakdown.length === 0 && (!near(totalNet, 0) || !near(totalVat, 0) || !near(totalGross, 0))) {
    fail('INVOICE_PRESENTATION_VAT_BREAKDOWN_MISSING', {
      total_net: totalNet,
      total_vat: totalVat,
      total_gross: totalGross
    });
  }
  let vatBreakdownNet = 0;
  let vatBreakdownVat = 0;
  let vatBreakdownGross = 0;
  const vatBreakdownKeys = new Set();

  for (const row of vatBreakdown) {
    requireObject(row, 'INVOICE_PRESENTATION_VAT_ROW_INVALID');
    for (const field of ['rate', 'net_amount', 'vat_amount', 'gross_amount']) {
      if (row[field] == null || row[field] === '') fail('INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING', `vat_breakdown[].${field}`);
    }
    const rate = asNumber(row.rate, 'INVOICE_PRESENTATION_NUMERIC_INVALID', 'vat_breakdown.rate');
    const net = assertFiniteMoney(row.net_amount, 'vat_breakdown.net_amount');
    const vat = assertFiniteMoney(row.vat_amount, 'vat_breakdown.vat_amount');
    const gross = assertFiniteMoney(row.gross_amount, 'vat_breakdown.gross_amount');
    if (!near(net + vat, gross)) fail('INVOICE_PRESENTATION_VAT_TOTAL_MISMATCH', rate);
    const key = String(rate);
    if (vatBreakdownKeys.has(key)) fail('INVOICE_PRESENTATION_VAT_RATE_DUPLICATE', rate);
    vatBreakdownKeys.add(key);
    vatBreakdownNet += net;
    vatBreakdownVat += vat;
    vatBreakdownGross += gross;
  }

  if (vatBreakdown.length > 0 && (!near(vatBreakdownNet, totalNet) || !near(vatBreakdownVat, totalVat) || !near(vatBreakdownGross, totalGross))) {
    fail('INVOICE_PRESENTATION_VAT_TOTAL_MISMATCH', {
      vat_breakdown_net: vatBreakdownNet,
      vat_breakdown_vat: vatBreakdownVat,
      vat_breakdown_gross: vatBreakdownGross
    });
  }

  if (Math.abs(totalVat) > 0.004 && !String(model.supplier.vat_registration_number || '').trim()) {
    fail('INVOICE_SUPPLIER_VAT_REGISTRATION_REQUIRED');
  }

  if (documentType === 'CREDIT_NOTE') {
    requireText(model.credit_note.original_invoice_id || model.credit_note.original_invoice_number, 'INVOICE_CREDIT_NOTE_RELATIONSHIP_MISSING');
    requireText(model.credit_note.original_invoice_number, 'INVOICE_CREDIT_NOTE_RELATIONSHIP_MISSING');
    requireText(model.credit_note.reason, 'INVOICE_CREDIT_NOTE_REASON_MISSING');
  }

  if (documentType === 'SELF_BILL_INVOICE') {
    requireText(model.self_bill.legal_wording, 'INVOICE_SELF_BILL_WORDING_MISSING');
  }

  requireOptionalTextArray(model.legal_wording, 'INVOICE_PRESENTATION_MODEL_INVALID', 'legal_wording');

  if (model.payment.hide_bank_footer === true) {
    const forbiddenPaymentFields = [
      'account_name',
      'sort_code',
      'account_number',
      'remittance_reference'
    ];
    for (const field of forbiddenPaymentFields) {
      if (String(model.payment[field] || '').trim()) {
        fail('INVOICE_PAYMENT_DETAILS_FORBIDDEN_BY_POLICY', field);
      }
    }
  }

  validateAssetIdentity(model.branding?.logo);
  rejectMutableUrls(model);
  assertNoPaySideFields(model);

  return model;
}

export function validateFrozenTimesheetPresentationModel(model, options = {}) {
  requireObject(model, 'RENDER_MODEL_INVALID');

  const fail = (code, detail) => { throw contractError(code, detail); };
  const requireOptionalArray = (value, code, detail) => {
    if (value == null) return [];
    if (!Array.isArray(value)) fail(code, detail);
    return value;
  };
  const finite = (value, code, detail) => {
    const number = Number(value);
    if (!Number.isFinite(number)) fail(code, detail);
    return number;
  };
  const validateReferenceArray = (value, label) => {
    const rows = requireOptionalArray(value, 'TIMESHEET_REFERENCE_MODEL_INVALID', label);
    const seen = new Set();
    rows.forEach((row, index) => {
      if (typeof row === 'string') {
        if (!row.trim()) fail('TIMESHEET_REFERENCE_MODEL_INVALID', `${label}[${index}]`);
        return;
      }
      requireObject(row, 'TIMESHEET_REFERENCE_MODEL_INVALID');
      const key = String(row.day_key || row.segment_id || row.row_key || `${label}:${index + 1}`);
      if (seen.has(key)) fail('TIMESHEET_REFERENCE_DUPLICATE', key);
      seen.add(key);
      if (!String(row.reference || row.current_reference || '').trim()) {
        fail('TIMESHEET_REFERENCE_MODEL_INVALID', `${label}[${index}].reference`);
      }
      if (row.display_order != null) {
        const order = Number(row.display_order);
        if (!Number.isSafeInteger(order) || order < 1) fail('TIMESHEET_REFERENCE_MODEL_INVALID', `${label}[${index}].display_order`);
      }
    });
    return rows;
  };
  const validateScheduleRows = (rows, label, requireRows) => {
    const arr = requireArray(rows, 'TIMESHEET_PRESENTATION_MODEL_INVALID');
    if (requireRows && arr.length === 0) fail('TIMESHEET_PRESENTATION_MODEL_INVALID', `${label}_missing`);
    const displayOrders = new Set();
    for (const row of arr) {
      requireObject(row, 'TIMESHEET_PRESENTATION_MODEL_INVALID');
      requireText(row.date, 'TIMESHEET_PRESENTATION_MODEL_INVALID');
      const order = Number(row.display_order);
      if (!Number.isSafeInteger(order) || order < 1) fail('TIMESHEET_PRESENTATION_MODEL_INVALID', `${label}.display_order`);
      if (displayOrders.has(order)) fail('TIMESHEET_PRESENTATION_DISPLAY_ORDER_DUPLICATE', `${label}.${order}`);
      displayOrders.add(order);

      const units = row.hours ?? row.units;
      const hasWorkedUnits = units != null && Number(units) > 0;
      if (hasWorkedUnits && (!String(row.worked_start || '').trim() || !String(row.worked_end || '').trim())) {
        fail('TIMESHEET_PRESENTATION_WORKED_TIME_MISSING', `${label}.${order}`);
      }
      if (units != null) finite(units, 'TIMESHEET_PRESENTATION_NUMERIC_INVALID', `${label}.${order}.units`);
      if (row.break_minutes != null) {
        const minutes = finite(row.break_minutes, 'TIMESHEET_PRESENTATION_NUMERIC_INVALID', `${label}.${order}.break_minutes`);
        if (minutes < 0) fail('TIMESHEET_PRESENTATION_BREAK_INVALID', `${label}.${order}`);
      }
    }
  };

  if (model.schema_version === 'TIMESHEET_RENDER_MODEL_V2') {
    return validateFrozenTimesheetPresentationModelV2(model, options);
  }

  if (model.schema_version !== 'TIMESHEET_RENDER_MODEL_V1') {
    fail('RENDER_MODEL_SCHEMA_UNSUPPORTED', model.schema_version);
  }

  requireText(model.timesheet_id, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.candidate, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.client, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.contract, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.work, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.references, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.authorisation, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.signatures, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.qr, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');

  requireText(model.candidate.name, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.client.name, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.week_ending_date, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.submission_mode, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.sheet_scope, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');

  const scope = String(model.sheet_scope || '').toUpperCase();
  validateScheduleRows(model.daily_schedule_rows, 'daily_schedule_rows', scope === 'DAILY');
  validateScheduleRows(model.weekly_schedule_rows, 'weekly_schedule_rows', scope === 'WEEKLY');

  if (scope !== 'DAILY' && scope !== 'WEEKLY') {
    const totalRows = Number((model.daily_schedule_rows || []).length + (model.weekly_schedule_rows || []).length);
    if (totalRows < 1) fail('TIMESHEET_PRESENTATION_MODEL_INVALID', 'schedule_rows_missing');
  }

  if (model.references.whole != null && typeof model.references.whole !== 'string') {
    fail('TIMESHEET_REFERENCE_MODEL_INVALID', 'references.whole');
  }
  validateReferenceArray(model.references.day, 'references.day');
  validateReferenceArray(model.references.segment, 'references.segment');

  if (model.additional_units != null) {
    if (Array.isArray(model.additional_units)) {
      const seenAdditional = new Set();
      model.additional_units.forEach((entry, index) => {
        if (entry == null || entry === '') return;
        if (typeof entry === 'string') {
          if (!entry.trim()) fail('TIMESHEET_ADDITIONAL_UNITS_INVALID', `additional_units[${index}]`);
          return;
        }
        requireObject(entry, 'TIMESHEET_ADDITIONAL_UNITS_INVALID');
        const key = String(entry.row_key || entry.type || entry.label || entry.description || index);
        if (seenAdditional.has(key)) fail('TIMESHEET_ADDITIONAL_UNITS_DUPLICATE', key);
        seenAdditional.add(key);
        const numericValue = entry.units ?? entry.hours ?? entry.quantity ?? null;
        if (numericValue != null && numericValue !== '') {
          finite(numericValue, 'TIMESHEET_PRESENTATION_NUMERIC_INVALID', `additional_units[${index}]`);
        }
      });
    } else if (typeof model.additional_units === 'object') {
      const numericValue = model.additional_units.units ?? model.additional_units.hours ?? model.additional_units.quantity ?? null;
      if (numericValue != null && numericValue !== '') {
        finite(numericValue, 'TIMESHEET_PRESENTATION_NUMERIC_INVALID', 'additional_units');
      }
    } else {
      finite(model.additional_units, 'TIMESHEET_PRESENTATION_NUMERIC_INVALID', 'additional_units');
    }
  }

  if (typeof model.authorisation.authorised !== 'boolean') {
    fail('TIMESHEET_AUTHORISATION_STATE_INVALID');
  }
  if (model.authorisation.authorised) {
    requireText(model.authorisation.authorised_at_utc, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  }
  if (model.authorisation.name != null && model.authorisation.name !== '') {
    requireText(model.authorisation.name, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  }
  if (model.authorisation.role != null && model.authorisation.role !== '') {
    requireText(model.authorisation.role, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  }

  validateAssetIdentity(model.signatures.candidate);
  validateAssetIdentity(model.signatures.authoriser);

  if (typeof model.qr.required !== 'boolean') fail('TIMESHEET_QR_STATE_INVALID', 'required');
  if (typeof model.qr.signed !== 'boolean') fail('TIMESHEET_QR_STATE_INVALID', 'signed');
  if (model.qr.signed && !String(model.qr.signed_hash || '').trim()) fail('TIMESHEET_QR_STATE_INVALID', 'signed_hash');
  if (model.qr.signed && !String(model.qr.signed_at_utc || '').trim()) fail('TIMESHEET_QR_STATE_INVALID', 'signed_at_utc');
  if (model.qr.required && String(model.qr.status || '').toUpperCase() === 'SIGNED' && model.qr.signed !== true) {
    fail('TIMESHEET_QR_STATE_INVALID', 'signed_status_mismatch');
  }

  if (options.templateVersion && model.template_version !== options.templateVersion) {
    fail('RENDER_TEMPLATE_VERSION_MISMATCH');
  }

  rejectMutableUrls(model);
  return model;
}

function parseYmd(value, code, detail) {
  const text = String(value || '').slice(0, 10);
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (!match) throw contractError(code, detail);
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  if (Number.isNaN(date.getTime())
    || date.getUTCFullYear() !== Number(match[1])
    || date.getUTCMonth() !== Number(match[2]) - 1
    || date.getUTCDate() !== Number(match[3])) {
    throw contractError(code, detail);
  }
  return { text, date };
}

function validateFrozenTimesheetPresentationModelV2(model, options = {}) {
  const fail = (code, detail) => { throw contractError(code, detail); };
  const finite = (value, code, detail) => {
    const number = Number(value);
    if (!Number.isFinite(number)) fail(code, detail);
    return number;
  };
  const forbiddenFinancialKeys = new Set([
    'pay_rate', 'charge_rate', 'pay_amount', 'charge_amount', 'margin',
    'vat', 'vat_rate', 'pay_method', 'total_pay_ex_vat',
    'total_charge_ex_vat', 'additional_pay_ex_vat',
    'additional_charge_ex_vat', 'additional_margin_ex_vat'
  ]);
  const rejectFinancialFields = (value, path = '$') => {
    if (Array.isArray(value)) {
      value.forEach((entry, index) => rejectFinancialFields(entry, `${path}[${index}]`));
      return;
    }
    if (!OBJECT(value)) return;
    for (const [key, entry] of Object.entries(value)) {
      if (forbiddenFinancialKeys.has(key)) {
        fail('TIMESHEET_ADDITIONAL_UNITS_FINANCIAL_FIELD_FORBIDDEN', `${path}.${key}`);
      }
      rejectFinancialFields(entry, `${path}.${key}`);
    }
  };

  requireText(model.timesheet_id, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.template_version, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.layout_contract_version, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.timesheet_number, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.sheet_scope, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.form_variant, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.submission_mode, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  if (String(model.locale || '') !== 'en-GB') fail('TIMESHEET_LOCALE_UNSUPPORTED');
  if (String(model.time_zone || '') !== 'Europe/London') fail('TIMESHEET_TIME_ZONE_UNSUPPORTED');
  if (!Number.isSafeInteger(Number(model.document_revision)) || Number(model.document_revision) < 1) {
    fail('TIMESHEET_DOCUMENT_REVISION_INVALID');
  }
  if (options.templateVersion && model.template_version !== options.templateVersion) {
    fail('RENDER_TEMPLATE_VERSION_MISMATCH', {
      expected: options.templateVersion,
      actual: model.template_version
    });
  }

  const worker = requireObject(model.worker, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  const client = requireObject(model.client, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(
    `${String(worker.first_name || '').trim()} ${String(worker.surname || '').trim()}`.trim(),
    'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING'
  );
  requireText(client.name || client.hospital, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireObject(model.branding, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  requireText(model.branding.agency_name, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  validateAssetIdentity(model.branding.logo);
  requireObject(model.wording, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');

  const period = requireObject(model.week_period, 'TIMESHEET_WEEK_PERIOD_INVALID');
  const start = parseYmd(period.start_date, 'TIMESHEET_WEEK_PERIOD_INVALID', 'start_date');
  const end = parseYmd(period.end_date, 'TIMESHEET_WEEK_PERIOD_INVALID', 'end_date');
  if ((end.date.getTime() - start.date.getTime()) / 86400000 !== 6) {
    fail('TIMESHEET_WEEK_PERIOD_INVALID', 'period_must_span_seven_calendar_dates');
  }
  if (Number(period.end_weekday_index) !== end.date.getUTCDay()) {
    fail('TIMESHEET_WEEK_ENDING_WEEKDAY_MISMATCH');
  }
  const weekdayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  if (String(period.end_weekday_name) !== weekdayNames[end.date.getUTCDay()]) {
    fail('TIMESHEET_WEEK_ENDING_WEEKDAY_MISMATCH');
  }
  if (period.configured_week_ending_weekday != null
    && Number(period.configured_week_ending_weekday) !== end.date.getUTCDay()) {
    fail('TIMESHEET_WEEK_ENDING_WEEKDAY_MISMATCH');
  }

  const days = requireArray(period.days, 'TIMESHEET_WEEK_PERIOD_INVALID');
  if (days.length !== 7) fail('TIMESHEET_WEEK_PERIOD_INVALID', 'exactly_seven_days_required');
  const dayKeys = new Set();
  const shiftKeys = new Set();
  let paidMinuteSum = 0;
  let previousShiftSortKey = '';
  for (let index = 0; index < days.length; index += 1) {
    const day = requireObject(days[index], 'TIMESHEET_WEEK_PERIOD_INVALID');
    const parsed = parseYmd(day.date, 'TIMESHEET_WEEK_PERIOD_INVALID', `days[${index}].date`);
    const expectedDate = new Date(start.date);
    expectedDate.setUTCDate(start.date.getUTCDate() + index);
    if (parsed.date.getTime() !== expectedDate.getTime()) {
      fail('TIMESHEET_WEEK_PERIOD_INVALID', `days[${index}].date_not_contiguous`);
    }
    const rowKey = requireText(day.row_key, 'TIMESHEET_WEEK_PERIOD_INVALID');
    if (dayKeys.has(rowKey)) fail('TIMESHEET_SCHEDULE_DUPLICATE_IDENTITY', rowKey);
    dayKeys.add(rowKey);
    if (Number(day.display_order) !== index + 1) {
      fail('TIMESHEET_PRESENTATION_DISPLAY_ORDER_INVALID', rowKey);
    }
    if (Number(day.weekday_index) !== parsed.date.getUTCDay()
      || String(day.weekday_name) !== weekdayNames[parsed.date.getUTCDay()]) {
      fail('TIMESHEET_WEEK_PERIOD_INVALID', `${rowKey}.weekday`);
    }
    const shifts = requireArray(day.shift_lines, 'TIMESHEET_PRESENTATION_MODEL_INVALID');
    previousShiftSortKey = '';
    for (let shiftIndex = 0; shiftIndex < shifts.length; shiftIndex += 1) {
      const shift = requireObject(shifts[shiftIndex], 'TIMESHEET_PRESENTATION_MODEL_INVALID');
      const shiftKey = requireText(shift.row_key, 'TIMESHEET_SCHEDULE_DUPLICATE_IDENTITY');
      if (shiftKeys.has(shiftKey)) fail('TIMESHEET_SCHEDULE_DUPLICATE_IDENTITY', shiftKey);
      shiftKeys.add(shiftKey);
      if (Number(shift.display_order) !== shiftIndex + 1) {
        fail('TIMESHEET_PRESENTATION_DISPLAY_ORDER_INVALID', shiftKey);
      }
      if (shift.date != null && String(shift.date).slice(0, 10) !== parsed.text) {
        fail('TIMESHEET_SCHEDULE_DATE_OUTSIDE_PERIOD', shiftKey);
      }
      const paidMinutes = finite(
        shift.paid_minutes,
        'TIMESHEET_PRESENTATION_NUMERIC_INVALID',
        `${shiftKey}.paid_minutes`
      );
      if (paidMinutes < 0) fail('TIMESHEET_PRESENTATION_NUMERIC_INVALID', shiftKey);
      if (paidMinutes > 0
        && (!String(shift.display_start_local || '').trim()
          || !String(shift.display_end_local || '').trim())) {
        fail('TIMESHEET_PRESENTATION_WORKED_TIME_MISSING', shiftKey);
      }
      const breakMinutes = finite(
        shift.break_minutes ?? 0,
        'TIMESHEET_PRESENTATION_NUMERIC_INVALID',
        `${shiftKey}.break_minutes`
      );
      if (breakMinutes < 0) fail('TIMESHEET_PRESENTATION_BREAK_INVALID', shiftKey);
      const breakMode = String(shift.break_display_mode || 'NONE');
      if (!['EXPLICIT_INTERVAL','MINUTES_ONLY','NONE'].includes(breakMode)) {
        fail('TIMESHEET_PRESENTATION_BREAK_INVALID', shiftKey);
      }
      if (breakMode === 'EXPLICIT_INTERVAL'
        && (!String(shift.break_start_local || '').trim()
          || !String(shift.break_end_local || '').trim())) {
        fail('TIMESHEET_PRESENTATION_BREAK_INVALID', shiftKey);
      }
      if (shift.reference_required === true && !String(shift.booking_reference || '').trim()) {
        fail('TIMESHEET_REFERENCE_MODEL_INVALID', shiftKey);
      }
      if (shift.booking_reference && !String(shift.reference_source || '').trim()) {
        fail('TIMESHEET_REFERENCE_MODEL_INVALID', `${shiftKey}.reference_source`);
      }
      const sortKey = `${String(shift.display_start_local || '')}|${String(shift.segment_id || shiftKey)}`;
      if (previousShiftSortKey && sortKey < previousShiftSortKey) {
        fail('TIMESHEET_PRESENTATION_DISPLAY_ORDER_INVALID', shiftKey);
      }
      previousShiftSortKey = sortKey;
      paidMinuteSum += paidMinutes;
    }
  }
  const totals = requireObject(model.totals, 'TIMESHEET_PRESENTATION_REQUIRED_FIELD_MISSING');
  const totalPaidMinutes = finite(
    totals.paid_minutes,
    'TIMESHEET_PRESENTATION_NUMERIC_INVALID',
    'totals.paid_minutes'
  );
  if (Math.abs(totalPaidMinutes - paidMinuteSum) > 0.001) {
    fail('TIMESHEET_PRESENTATION_TOTAL_MISMATCH', {
      expected: paidMinuteSum,
      actual: totalPaidMinutes
    });
  }

  const additional = requireObject(
    model.additional_units_section,
    'TIMESHEET_ADDITIONAL_UNITS_INVALID'
  );
  if (additional.schema_version !== 'TIMESHEET_ADDITIONAL_UNITS_V1') {
    fail('TIMESHEET_ADDITIONAL_UNITS_INVALID', 'schema_version');
  }
  const additionalRows = requireArray(additional.rows, 'TIMESHEET_ADDITIONAL_UNITS_INVALID');
  if (additional.visible !== (additionalRows.length > 0)) {
    fail('TIMESHEET_ADDITIONAL_UNITS_INVALID', 'visibility');
  }
  const additionalKeys = new Set();
  const additionalOrders = new Set();
  for (const row of additionalRows) {
    requireObject(row, 'TIMESHEET_ADDITIONAL_UNITS_INVALID');
    const rowKey = requireText(row.row_key, 'TIMESHEET_ADDITIONAL_UNITS_INVALID');
    if (additionalKeys.has(rowKey)) fail('TIMESHEET_ADDITIONAL_UNIT_IDENTITY_DUPLICATE', rowKey);
    additionalKeys.add(rowKey);
    const displayOrder = Number(row.display_order);
    if (!Number.isSafeInteger(displayOrder) || displayOrder < 1 || additionalOrders.has(displayOrder)) {
      fail('TIMESHEET_ADDITIONAL_UNIT_IDENTITY_DUPLICATE', displayOrder);
    }
    additionalOrders.add(displayOrder);
    requireText(row.code, 'TIMESHEET_ADDITIONAL_UNITS_INVALID');
    requireText(row.rate_type, 'TIMESHEET_ADDITIONAL_UNITS_INVALID');
    requireText(row.unit, 'TIMESHEET_ADDITIONAL_UNITS_INVALID');
    const quantity = finite(row.quantity, 'TIMESHEET_ADDITIONAL_UNITS_INVALID', `${rowKey}.quantity`);
    if (quantity === 0) fail('TIMESHEET_ADDITIONAL_UNITS_INVALID', `${rowKey}.quantity_zero`);
    if (!['WEEKLY','PER_DAY'].includes(String(row.frequency || '').toUpperCase())) {
      fail('TIMESHEET_ADDITIONAL_UNITS_INVALID', `${rowKey}.frequency`);
    }
    if (row.date != null) {
      const date = parseYmd(
        row.date,
        'TIMESHEET_ADDITIONAL_UNIT_DATE_OUTSIDE_PERIOD',
        `${rowKey}.date`
      );
      if (date.date < start.date || date.date > end.date) {
        fail('TIMESHEET_ADDITIONAL_UNIT_DATE_OUTSIDE_PERIOD', rowKey);
      }
    }
  }
  rejectFinancialFields(additional);

  const layout = requireObject(model.layout, 'TIMESHEET_ONE_PAGE_CONTRACT_INVALID');
  if (layout.one_page_required !== true || layout.second_page_allowed !== false) {
    fail('TIMESHEET_ONE_PAGE_CONTRACT_INVALID');
  }
  const modes = requireArray(layout.allowed_modes, 'TIMESHEET_ONE_PAGE_CONTRACT_INVALID');
  if (!modes.length || modes.some(mode => !['NORMAL','COMPACT','ULTRA'].includes(String(mode)))) {
    fail('TIMESHEET_ONE_PAGE_CONTRACT_INVALID', 'allowed_modes');
  }
  for (const field of [
    'minimum_font_size',
    'minimum_row_height_mm',
    'minimum_signature_height_mm',
    'minimum_additional_blank_rows'
  ]) {
    if (finite(layout[field], 'TIMESHEET_ONE_PAGE_CONTRACT_INVALID', field) < 0) {
      fail('TIMESHEET_ONE_PAGE_CONTRACT_INVALID', field);
    }
  }

  const signatures = requireObject(model.signatures, 'TIMESHEET_SIGNATURE_ASSET_INVALID');
  const qr = requireObject(model.qr, 'TIMESHEET_QR_STATE_INVALID');
  const variant = String(model.form_variant || '');
  if (!['ELECTRONIC_SIGNED','ELECTRONIC_UNSIGNED','QR_UNSIGNED'].includes(variant)) {
    fail('TIMESHEET_FORM_VARIANT_INVALID', variant);
  }
  validateAssetIdentity(signatures.candidate);
  validateAssetIdentity(signatures.authoriser);
  if (variant === 'ELECTRONIC_SIGNED') {
    requireText(model.authorisation?.authorised_at_utc, 'TIMESHEET_AUTHORISATION_STATE_INVALID');
    if (!signatures.candidate?.r2_key || !signatures.authoriser?.r2_key) {
      fail('TIMESHEET_SIGNATURE_ASSET_INVALID');
    }
    if (qr.required === true) fail('TIMESHEET_QR_STATE_INVALID', 'electronic_signed_has_qr');
  } else if (variant === 'ELECTRONIC_UNSIGNED') {
    if (qr.required === true) fail('TIMESHEET_QR_STATE_INVALID', 'electronic_unsigned_has_qr');
  } else {
    requireText(qr.token, 'TIMESHEET_QR_STATE_INVALID');
    requireObject(qr.payload, 'TIMESHEET_QR_STATE_INVALID');
    if (qr.required !== true || signatures.candidate?.r2_key || signatures.authoriser?.r2_key) {
      fail('TIMESHEET_QR_STATE_INVALID');
    }
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
  if (model?.schema_version === 'HEALTHROSTER_PRESENTATION_V2') {
    const validated = validateSupportModel(model, 'HEALTHROSTER_PRESENTATION_V2', [
      'worker', 'assignment', 'shift_date', 'shift_times', 'site', 'ward',
      'booking_reference', 'units_hours', 'validation_state', 'source_identity',
      'reference_match_state', 'reference_match_count'
    ]);
    for (const row of validated.rows) {
      const matchCount = Number(row.reference_match_count);
      if (!Number.isSafeInteger(matchCount) || matchCount < 0) {
        throw contractError('HEALTHROSTER_REFERENCE_MATCH_INVALID');
      }
      if (String(row.reference_match_state || '').toUpperCase() === 'AMBIGUOUS'
        || matchCount > 1) {
        throw contractError('HEALTHROSTER_REFERENCE_MATCH_AMBIGUOUS');
      }
      if (matchCount === 1 && !String(row.booking_reference || '').trim()) {
        throw contractError('HEALTHROSTER_REFERENCE_MATCH_INVALID');
      }
    }
    return validated;
  }
  return validateSupportModel(model, 'HEALTHROSTER_PRESENTATION_V1', [
    'worker', 'assignment', 'shift_date', 'shift_times', 'site', 'ward',
    'reference', 'units_hours', 'validation_state', 'source_identity'
  ]);
}

export function validateFrozenNhspModel(model) {
  return validateSupportModel(model, 'NHSP_PRESENTATION_V1', [
    'worker', 'nhsp_shift_id', 'booking_reference', 'site_ward', 'shift_date',
    'shift_times', 'hours_units', 'commission_amount', 'total_amount',
    'source_identity', 'validation_state'
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
