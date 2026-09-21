const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const DECIMAL = /^-?(?:0|[1-9]\d*)(?:\.(\d+))?$/;
const EXPENSE_CODES = Object.freeze([
  ['TRAVEL', 'travel_pay_ex_vat', 'travel_charge_ex_vat'],
  ['ACCOMMODATION', 'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat'],
  ['OTHER', 'other_pay_ex_vat', 'other_charge_ex_vat'],
  ['MILEAGE', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat'],
]);
const encoder = new TextEncoder();

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function object(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(code, `${label} is unavailable.`);
  return value;
}

function uuid(value, label) {
  const result = String(value ?? '').trim().toLowerCase();
  if (!UUID.test(result)) fail('C1_COMPONENT_UUID_INVALID', `${label} is invalid.`);
  return result;
}

function text(value, label) {
  const result = String(value ?? '').trim();
  if (!result) fail('C1_COMPONENT_TEXT_INVALID', `${label} is unavailable.`);
  return result;
}

function canonicalDecimal(value, scale, label, { nullable = false } = {}) {
  if (value == null || value === '') {
    if (nullable) return null;
    fail('C1_COMPONENT_DECIMAL_INVALID', `${label} is unavailable.`);
  }
  const token = typeof value === 'number' ? String(value) : String(value).trim();
  const match = token.match(DECIMAL);
  if (!match || !Number.isFinite(Number(token)) || Object.is(Number(token), -0)) {
    fail('C1_COMPONENT_DECIMAL_INVALID', `${label} is invalid.`);
  }
  const fraction = match[1] ?? '';
  if (fraction.length > scale && /[1-9]/.test(fraction.slice(scale))) {
    fail('C1_COMPONENT_DECIMAL_PRECISION', `${label} has more precision than its financial authority.`);
  }
  return Number(token).toFixed(scale);
}

function nonZeroMoney(value) {
  return BigInt(String(value).replace('.', '')) !== 0n;
}

function moneyMinorUnits(value, label) {
  const canonical = canonicalDecimal(value, 2, label);
  return BigInt(canonical.replace('.', ''));
}

function rawCodeKey(rootId, family, rawCode) {
  const bytes = encoder.encode(rawCode);
  const hex = [...bytes].map((value) => value.toString(16).padStart(2, '0')).join('');
  return `10:${rootId}:${family}:${String(bytes.byteLength).padStart(10, '0')}:${hex}`;
}

function commonComponent(overrides) {
  return {
    component_id: null,
    source_ordinal: null,
    source_key: null,
    component_kind: null,
    economic_key_type: null,
    economic_key_value: null,
    component_member_identity: null,
    segment_id: null,
    segment_key: null,
    segment_stable_key: null,
    work_date: null,
    reference_number: null,
    hours_day: null,
    hours_night: null,
    hours_sat: null,
    hours_sun: null,
    hours_bh: null,
    additional_code_raw: null,
    unit_count: null,
    unit_pay_rate: null,
    unit_charge_rate: null,
    expense_code: null,
    // S7 (WB-007, WB-013, 24 section 5): no 'adjustment_id' field.  The SQL
    // owner uses an exact-key component allowlist that rejects the key.
    pay_ex_vat: '0.00',
    charge_ex_vat: null,
    exclude_from_pay: false,
    origin: 'WEEKLY_SOURCE_APPROVED_TARGET',
    ...overrides,
  };
}

function workedComponents(rootId, snapshot) {
  const breakdown = object(
    snapshot.invoice_breakdown_json,
    'C1_COMPONENT_BREAKDOWN_INVALID',
    'Financial segment evidence',
  );
  if (String(breakdown.mode ?? '').trim().toUpperCase() !== 'SEGMENTS' || !Array.isArray(breakdown.segments)) {
    fail('C1_COMPONENT_BREAKDOWN_INVALID', 'Financial segment evidence is not a complete segment list.');
  }
  return breakdown.segments.map((raw, index) => {
    const segment = object(raw, 'C1_COMPONENT_SEGMENT_INVALID', 'Financial segment');
    const date = text(segment.date, 'Financial segment date');
    if (!DATE.test(date)) fail('C1_COMPONENT_DATE_INVALID', 'Financial segment date is invalid.');
    const segmentId = text(segment.segment_id, 'Financial segment identity');
    const segmentKey = text(segment.segment_key ?? segmentId, 'Financial segment key');
    const stableKey = text(
      segment.segment_stable_key ?? segment.segment_id ?? segment.segment_key,
      'Stable financial segment key',
    );
    const exclude = segment.exclude_from_pay === true;
    return commonComponent({
      source_key: `10:${rootId}:SEGMENT:${String(index + 1).padStart(12, '0')}`,
      component_kind: 'WORKED_TIME',
      economic_key_type: 'TS_DAY',
      economic_key_value: date,
      component_member_identity: stableKey,
      segment_id: segmentId,
      segment_key: segmentKey,
      segment_stable_key: stableKey,
      work_date: date,
      reference_number: segment.ref_num == null || String(segment.ref_num).trim() === ''
        ? null
        : String(segment.ref_num).trim(),
      hours_day: canonicalDecimal(segment.hours_day ?? 0, 6, 'Day hours'),
      hours_night: canonicalDecimal(segment.hours_night ?? 0, 6, 'Night hours'),
      hours_sat: canonicalDecimal(segment.hours_sat ?? 0, 6, 'Saturday hours'),
      hours_sun: canonicalDecimal(segment.hours_sun ?? 0, 6, 'Sunday hours'),
      hours_bh: canonicalDecimal(segment.hours_bh ?? 0, 6, 'Bank-holiday hours'),
      pay_ex_vat: canonicalDecimal(exclude ? 0 : segment.pay_amount, 2, 'Segment pay'),
      charge_ex_vat: canonicalDecimal(segment.charge_amount, 2, 'Segment charge', { nullable: true }),
      exclude_from_pay: exclude,
    });
  });
}

function additionalComponents(rootId, snapshot) {
  const input = snapshot.additional_units_json ?? {};
  object(input, 'C1_COMPONENT_ADDITIONAL_INVALID', 'Additional-unit evidence');
  return Object.entries(input).map(([rawCode, raw]) => {
    const code = text(rawCode, 'Additional-unit code');
    const value = object(raw, 'C1_COMPONENT_ADDITIONAL_INVALID', 'Additional-unit evidence');
    return commonComponent({
      source_key: rawCodeKey(rootId, 'ADDITIONAL', code),
      component_kind: 'ADDITIONAL_UNIT',
      economic_key_type: 'ADDITIONAL_CODE',
      economic_key_value: code.toUpperCase(),
      component_member_identity: `additional:${code.toUpperCase()}`,
      additional_code_raw: code,
      unit_count: canonicalDecimal(value.unit_count ?? value.units_week, 6, 'Additional-unit count'),
      unit_pay_rate: canonicalDecimal(value.pay_rate ?? value.rate, 6, 'Additional-unit pay rate'),
      unit_charge_rate: canonicalDecimal(value.charge_rate, 6, 'Additional-unit charge rate', { nullable: true }),
      pay_ex_vat: canonicalDecimal(value.pay_ex_vat ?? value.amount_ex_vat, 2, 'Additional-unit pay'),
      charge_ex_vat: canonicalDecimal(
        value.charge_ex_vat ?? value.charge_amount_ex_vat,
        2,
        'Additional-unit charge',
        { nullable: true },
      ),
    });
  }).filter((component) => nonZeroMoney(component.pay_ex_vat));
}

function expenseComponents(rootId, snapshot, suppliedExpenses) {
  const supplied = Array.isArray(suppliedExpenses) ? suppliedExpenses : [];
  const byCode = new Map();
  for (const raw of supplied) {
    const row = object(raw, 'C1_COMPONENT_EXPENSE_INVALID', 'Expense evidence');
    const code = text(row.expense_code, 'Expense code').toUpperCase();
    if (byCode.has(code)) fail('C1_COMPONENT_EXPENSE_DUPLICATE', 'An expense category was supplied more than once.');
    byCode.set(code, row);
  }

  const suppliedSourceRows = [...byCode.values()].filter(
    (row) => String(row.authority_kind ?? '').trim().toUpperCase() === 'SOURCE_EXPENSE',
  );
  const suppliedSourcePayMinor = suppliedSourceRows.reduce(
    (total, row) => total + moneyMinorUnits(row.pay_ex_vat, 'Source-approved expense pay'),
    0n,
  );
  const suppliedSourceChargeMinor = suppliedSourceRows.reduce(
    (total, row) => total + moneyMinorUnits(row.charge_ex_vat, 'Source-approved expense charge'),
    0n,
  );

  for (const [code, payField, chargeField] of EXPENSE_CODES) {
    const pay = canonicalDecimal(snapshot[payField] ?? 0, 2, `${code} pay`);
    const charge = canonicalDecimal(snapshot[chargeField], 2, `${code} charge`, { nullable: true });
    if (nonZeroMoney(pay)) {
      if (byCode.has(code)) fail('C1_COMPONENT_EXPENSE_DUPLICATE', 'An expense category has two authorities.');
      byCode.set(code, { expense_code: code, pay_ex_vat: pay, charge_ex_vat: charge, authority_kind: 'ORDINARY_EXPENSE' });
    }
  }

  const categorized = [...byCode.keys()].some((code) => code !== 'EXPENSES');
  const genericPay = canonicalDecimal(snapshot.expenses_pay_ex_vat ?? 0, 2, 'Generic expense pay');
  if (nonZeroMoney(genericPay)) {
    const genericCharge = canonicalDecimal(
      snapshot.expenses_charge_ex_vat,
      2,
      'Generic expense charge',
      { nullable: true },
    );
    const sourceExpenseOwnsGenericTotal = suppliedSourceRows.length > 0
      && moneyMinorUnits(genericPay, 'Generic expense pay') === suppliedSourcePayMinor
      && genericCharge !== null
      && moneyMinorUnits(genericCharge, 'Generic expense charge') === suppliedSourceChargeMinor;
    if (sourceExpenseOwnsGenericTotal) {
      // A configured source-supplied expense is stored in the ordinary Weekly
      // TSFIN aggregate, but its C1 evidence remains the immutable source
      // authority.  Do not emit the same amount again as an ordinary expense.
      // Multiple source rows retain distinct work-event-scoped expense codes.
    } else if (categorized || byCode.has('EXPENSES')) {
      fail('C1_EXPENSE_OWNERSHIP_CONFLICT', 'Generic and categorized expense authorities cannot overlap.');
    } else {
      byCode.set('EXPENSES', {
        expense_code: 'EXPENSES',
        pay_ex_vat: genericPay,
        charge_ex_vat: genericCharge,
        authority_kind: 'ORDINARY_EXPENSE',
      });
    }
  }

  return [...byCode.values()].map((row) => {
    const code = text(row.expense_code, 'Expense code').toUpperCase();
    const pay = canonicalDecimal(row.pay_ex_vat, 2, `${code} pay`);
    if (!nonZeroMoney(pay)) return null;
    return {
      component: commonComponent({
        source_key: `10:${rootId}:EXPENSE:${code}`,
        component_kind: 'EXPENSE',
        economic_key_type: 'EXPENSE_CODE',
        economic_key_value: code,
        component_member_identity: `expense:${code}`,
        expense_code: code,
        pay_ex_vat: pay,
        charge_ex_vat: canonicalDecimal(row.charge_ex_vat, 2, `${code} charge`, { nullable: true }),
      }),
      authority: Object.freeze({
        kind: String(row.authority_kind ?? 'ORDINARY_EXPENSE').trim().toUpperCase(),
        source_expense_id: row.source_expense_id == null ? null : uuid(row.source_expense_id, 'Source expense'),
        document_sha256: row.document_sha256 ?? null,
      }),
    };
  }).filter(Boolean);
}

// S7 (WB-007, WB-013, 24 section 5): there is no adjustment component builder.
// "Adjustments are never copied into the immutable head, because an adjustment
// created later would make that snapshot stale and create a second owner."  An
// ordinary non-advance ts_pay_adjustments occurrence stays independently owned
// and is composed exactly once by the Workbench selector, outside the head.

function normalizedComponentRows(input = {}) {
  const rootId = uuid(input.root_timesheet_id, 'Root Timesheet');
  const snapshot = object(input.financial_snapshot, 'C1_COMPONENT_SNAPSHOT_INVALID', 'Financial snapshot');
  if (uuid(snapshot.timesheet_id, 'Financial snapshot Timesheet') !== rootId) {
    fail('C1_COMPONENT_ROOT_MISMATCH', 'The financial snapshot belongs to another Timesheet.');
  }
  return {
    rootId,
    rows: [
      ...workedComponents(rootId, snapshot).map((component) => ({ component, authority: { kind: 'APPROVED_COMPONENT' } })),
      ...additionalComponents(rootId, snapshot).map((component) => ({ component, authority: { kind: 'APPROVED_COMPONENT' } })),
      ...expenseComponents(rootId, snapshot, input.expenses),
    ].sort((left, right) => left.component.source_key.localeCompare(right.component.source_key)),
  };
}

/**
 * Return the exact canonical keys that need service-owned component UUIDs.
 * This lets an asynchronous cryptographic identity factory resolve every ID
 * before the synchronous, closed component copier runs.
 */
export function listWeeklySourceC1ComponentKeys(input = {}) {
  const { rows } = normalizedComponentRows(input);
  const keys = rows.map((row) => row.component.source_key);
  if (new Set(keys).size !== keys.length) {
    fail('C1_COMPONENT_KEY_DUPLICATE', 'A physical entitlement key was supplied more than once.');
  }
  return Object.freeze(keys);
}

/**
 * Convert one complete server-calculated Weekly financial snapshot into the
 * exact physical component population consumed by C1.  It copies established
 * financial authority; it never calculates a residual, recovery or Draft.
 */
export function buildWeeklySourceC1Components(input = {}) {
  const { rows } = normalizedComponentRows(input);

  const seen = new Set();
  const componentIdForKey = input.component_id_for_key;
  if (typeof componentIdForKey !== 'function') {
    fail('C1_COMPONENT_ID_FACTORY_REQUIRED', 'The trusted component identity factory is unavailable.');
  }
  const output = rows.map((row, index) => {
    const key = row.component.source_key;
    if (seen.has(key)) fail('C1_COMPONENT_KEY_DUPLICATE', 'A physical entitlement key was supplied more than once.');
    seen.add(key);
    return Object.freeze({
      component: Object.freeze({
        ...row.component,
        component_ordinal: index + 1,
        component_id: uuid(componentIdForKey(key), 'Component'),
      }),
      authority: Object.freeze(row.authority),
    });
  });
  return Object.freeze(output);
}

export const WEEKLY_SOURCE_C1_COMPONENT_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_C1_COMPONENTS_V1',
  sourceModes: Object.freeze(['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY']),
  bankingClassificationForConfigurableRoster: 'HEALTHROSTER_WEEKLY',
});
