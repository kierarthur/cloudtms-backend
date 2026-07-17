const trim = (value) => String(value == null ? '' : value).trim();
const upper = (value) => trim(value).toUpperCase();

export function normaliseBulkAuthoriseEvidenceKind(value) {
  const kind = upper(value);
  if (kind === 'EXPENSE' || kind === 'EXPENSES') return 'TRAVEL';
  if (kind === 'MILE' || kind === 'MILES') return 'MILEAGE';
  if (kind === 'ACCOM') return 'ACCOMMODATION';
  if (kind === 'TS' || kind === 'TIME SHEET' || kind === 'TIME-SHEET') return 'TIMESHEET';
  return kind;
}

export function collectBulkAuthoriseInvoiceIds(financialRow) {
  const ids = new Set();
  const add = (value) => {
    const id = trim(value);
    if (id) ids.add(id);
  };
  add(financialRow?.locked_by_invoice_id);

  let breakdown = financialRow?.invoice_breakdown_json;
  if (typeof breakdown === 'string') {
    try { breakdown = JSON.parse(breakdown); } catch { breakdown = null; }
  }
  const segments = Array.isArray(breakdown)
    ? breakdown
    : (Array.isArray(breakdown?.segments) ? breakdown.segments : []);
  for (const segment of segments) add(segment?.invoice_locked_invoice_id ?? segment?.locked_by_invoice_id);
  return [...ids];
}

export function classifyBulkAuthoriseInvoiceEvidencePolicy(invoiceIds, invoiceRows) {
  const ids = [...new Set((Array.isArray(invoiceIds) ? invoiceIds : []).map(trim).filter(Boolean))];
  const rows = Array.isArray(invoiceRows) ? invoiceRows : [];
  if (!ids.length) {
    return { allowed: true, invoice_ids: [], mutable_invoice_ids: [], blocking_invoice_ids: [], reason: null };
  }

  const byId = new Map(rows.map((row) => [trim(row?.id), row]).filter(([id]) => id));
  const mutable = [];
  const blocking = [];
  for (const id of ids) {
    const row = byId.get(id);
    const status = upper(row?.status);
    const unissuedDraft = !!row && !trim(row?.issued_at_utc) && (status === 'DRAFT' || status === 'ON_HOLD');
    if (unissuedDraft) mutable.push(id);
    else blocking.push(id);
  }
  return {
    allowed: blocking.length === 0,
    invoice_ids: ids,
    mutable_invoice_ids: mutable,
    blocking_invoice_ids: blocking,
    reason: blocking.length ? 'INVOICE_ISSUED_OR_STATE_UNVERIFIED' : null
  };
}

const positive = (value) => Number.isFinite(Number(value)) && Number(value) > 0;

export function requiredBulkAuthoriseExpenseEvidenceKinds(financialRow) {
  const required = [];
  if (
    positive(financialRow?.mileage_units) ||
    positive(financialRow?.mileage_pay_ex_vat) ||
    positive(financialRow?.mileage_charge_ex_vat)
  ) required.push('MILEAGE');
  if (positive(financialRow?.travel_pay_ex_vat) || positive(financialRow?.travel_charge_ex_vat)) required.push('TRAVEL');
  if (positive(financialRow?.accommodation_pay_ex_vat) || positive(financialRow?.accommodation_charge_ex_vat)) required.push('ACCOMMODATION');
  if (positive(financialRow?.other_pay_ex_vat) || positive(financialRow?.other_charge_ex_vat)) required.push('OTHER');
  return required;
}

export function missingBulkAuthoriseExpenseEvidenceKinds(financialRow, evidenceRows) {
  const present = new Set((Array.isArray(evidenceRows) ? evidenceRows : [])
    .map((row) => normaliseBulkAuthoriseEvidenceKind(row?.kind))
    .filter(Boolean));
  return requiredBulkAuthoriseExpenseEvidenceKinds(financialRow).filter((kind) => !present.has(kind));
}
