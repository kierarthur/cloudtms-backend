// Offline adversarial comparison oracle. It consumes supplied native IDs and
// frozen facts; it does not derive entitlement IDs, prices, capability or state.
// PostgreSQL/native owner execution remains the mandatory integration proof.
import { isDeepStrictEqual } from 'node:util';

const identity = ['component_kind', 'economic_key_type', 'economic_key_value',
  'component_member_identity', 'segment_id', 'segment_key', 'work_date',
  'reference_number', 'additional_code_raw', 'expense_code', 'exclude_from_pay',
  'origin', 'movement_id', 'movement_group_id'];
const decimalFields = Object.freeze({ hours_day: 6, hours_night: 6, hours_sat: 6,
  hours_sun: 6, hours_bh: 6, unit_count: 6, unit_pay_rate: 6, unit_charge_rate: 6,
  pay_ex_vat: 2, charge_ex_vat: 2 });
const bands = ['day', 'night', 'sat', 'sun', 'bh'];
function fail(code) { const error = new Error(code); error.code = code; throw error; }
function integer(value, scale) {
  if (value === null) return null;
  if (typeof value !== 'string' || !/^-?(0|[1-9]\d*)(\.\d+)?$/.test(value)) fail('UNKNOWN_DECIMAL');
  const negative = value.startsWith('-');
  const [whole, fraction = ''] = value.replace(/^-/, '').split('.');
  if (fraction.length > scale && /[1-9]/.test(fraction.slice(scale))) fail('UNAPPROVED_PRECISION');
  const result = BigInt(whole + fraction.slice(0, scale).padEnd(scale, '0'));
  if (negative && result === 0n) fail('NEGATIVE_ZERO');
  return negative ? -result : result;
}
function indexed(rows) {
  if (!Array.isArray(rows)) fail('MISSING_COMPLETE_COMPONENTS');
  const result = new Map();
  for (const row of rows) {
    if (!row || !/^[0-9a-f-]{36}$/.test(row.component_id ?? '') || result.has(row.component_id)) fail('AMBIGUOUS_COMPONENT_ID');
    result.set(row.component_id, row);
  }
  return result;
}
export function compareCompleteNativeComponents(before, next, detailPairs, expensePolicyPairs = []) {
  const old = indexed(before); const proposed = indexed(next);
  if (old.size !== proposed.size) fail('COMPONENT_SET_CHANGED');
  for (const [id, row] of old) {
    const target = proposed.get(id); if (!target) fail('COMPONENT_SET_CHANGED');
    for (const field of identity) {
      if (!Object.hasOwn(row, field) || !Object.hasOwn(target, field)) fail('INCOMPLETE_NATIVE_IDENTITY');
      if (!isDeepStrictEqual(row[field], target[field])) fail('COMPONENT_IDENTITY_CHANGED');
    }
    for (const [field, scale] of Object.entries(decimalFields)) {
      if (!Object.hasOwn(row, field) || !Object.hasOwn(target, field)) fail('UNKNOWN_DECIMAL');
      if (integer(row[field], scale) !== integer(target[field], scale)) fail('COMPONENT_DELTA_NONZERO');
    }
    if (row.component_kind === 'WORKED_TIME') {
      const pair = detailPairs.get(id); if (!pair) fail('APPROVED_DETAIL_UNAVAILABLE');
      for (const field of ['work_date', 'start', 'end', 'overnight', 'breaks', 'start_utc', 'end_utc']) {
        if (!Object.hasOwn(pair.before, field) || !Object.hasOwn(pair.next, field)
          || !isDeepStrictEqual(pair.before[field], pair.next[field])) fail('APPROVED_DETAIL_CHANGED');
      }
      for (const band of bands) {
        const used = integer(row[`hours_${band}`], 6) !== 0n;
        for (const field of ['rates', 'charge_rates']) {
          const left = pair.before[field]?.[band]; const right = pair.next[field]?.[band];
          if (used && (left == null || right == null) && (field === 'charge_rates' || !row.exclude_from_pay)) fail('APPROVED_RATE_UNAVAILABLE');
          if (integer(left, 6) !== integer(right, 6)) fail('APPROVED_RATE_CHANGED');
        }
      }
    } else if (!['ADDITIONAL_UNIT', 'SOURCE_FIXED_EXPENSE'].includes(row.component_kind)) fail('COMPONENT_UNMAPPED');
  }
  for (const pair of expensePolicyPairs) if (!isDeepStrictEqual(pair.before, pair.next)) fail('EXPENSE_POLICY_CHANGED');
  return { component_count: old.size, delta_pay_ex_vat: '0.00' };
}
