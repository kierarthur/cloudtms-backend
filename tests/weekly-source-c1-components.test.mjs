import assert from 'node:assert/strict';
import test from 'node:test';

import { buildWeeklySourceC1Components } from '../broker/src/banking-pay/weekly-source-c1-components.mjs';

const ROOT = '10000000-0000-4000-8000-000000000001';
const ADJUSTMENT = '20000000-0000-4000-8000-000000000001';
const ids = new Map();
let next = 1;
const idForKey = (key) => {
  if (!ids.has(key)) ids.set(key, `30000000-0000-4000-8000-${String(next++).padStart(12, '0')}`);
  return ids.get(key);
};

function snapshot(overrides = {}) {
  return {
    timesheet_id: ROOT,
    additional_units_json: {
      'On Call': {
        unit_count: 1,
        pay_rate: 10,
        charge_rate: 12,
        pay_ex_vat: 10,
        charge_ex_vat: 12,
      },
    },
    expenses_pay_ex_vat: 0,
    expenses_charge_ex_vat: 0,
    mileage_pay_ex_vat: 0,
    mileage_charge_ex_vat: 0,
    invoice_breakdown_json: {
      mode: 'SEGMENTS',
      segments: [{
        segment_id: 'segment-1',
        segment_key: 'date:2026-09-14',
        segment_stable_key: 'event:one',
        date: '2026-09-14',
        ref_num: 'REF-1',
        hours_day: 7.5,
        hours_night: 0,
        hours_sat: 0,
        hours_sun: 0,
        hours_bh: 0,
        pay_amount: 100,
        charge_amount: 200,
        exclude_from_pay: false,
      }],
    },
    ...overrides,
  };
}

test('copies worked time, additional units and expenses into exact sorted physical keys, and never an adjustment', () => {
  const result = buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot(),
    expenses: [{
      authority_kind: 'SOURCE_EXPENSE',
      source_expense_id: '40000000-0000-4000-8000-000000000001',
      expense_code: 'OTHER',
      pay_ex_vat: 25,
      charge_ex_vat: 25,
    }],
    // S7 (WB-007, WB-013, 24 section 5): "Adjustments are never copied into the
    // immutable head."  An ordinary non-advance ts_pay_adjustments occurrence
    // stays independently owned and is composed exactly once by the Workbench
    // selector, outside the head, so supplying one here composes nothing.
    adjustments: [{ adjustment_id: ADJUSTMENT, pay_ex_vat: -5, as_advance: false }],
    component_id_for_key: idForKey,
  });
  assert.deepEqual(result.map((row) => row.component.component_kind), [
    'ADDITIONAL_UNIT', 'EXPENSE', 'WORKED_TIME',
  ]);
  assert.deepEqual(result.map((row) => row.component.component_ordinal), [1, 2, 3]);
  assert.equal(result.some((row) => row.authority.kind === 'NONADVANCE_ADJUSTMENT'), false);
  assert.equal(result.every((row) => !('adjustment_id' in row.component)), true);
  assert.equal(result[0].component.additional_code_raw, 'On Call');
  assert.equal(result[0].component.unit_pay_rate, '10.000000');
  assert.equal(result[1].authority.kind, 'SOURCE_EXPENSE');
  assert.equal(result[2].component.hours_day, '7.500000');
  assert.equal(result[2].component.pay_ex_vat, '100.00');
});

test('copies an established excluded segment as zero candidate pay without erasing its charge', () => {
  const source = snapshot();
  source.invoice_breakdown_json.segments[0].exclude_from_pay = true;
  const result = buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: source,
    component_id_for_key: idForKey,
  });
  assert.equal(result.at(-1).component.pay_ex_vat, '0.00');
  assert.equal(result.at(-1).component.charge_ex_vat, '200.00');
  assert.equal(result.at(-1).component.exclude_from_pay, true);
});

test('refuses duplicate generic/category expenses and composes no adjustment, advance or not', () => {
  assert.throws(() => buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot({ expenses_pay_ex_vat: 10 }),
    expenses: [{ expense_code: 'TRAVEL', pay_ex_vat: 5, charge_ex_vat: 5 }],
    component_id_for_key: idForKey,
  }), { code: 'C1_EXPENSE_OWNERSHIP_CONFLICT' });

  // S7: after the adjustment builder was removed, neither an advance nor a
  // non-advance adjustment can reach the head at all.  The head is byte-for-byte
  // what it is with no adjustments supplied.  WB-013 also keeps Weekly Source
  // from creating, targeting, changing or consuming any advance marker.
  const withAdvance = buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot(),
    adjustments: [{ adjustment_id: ADJUSTMENT, pay_ex_vat: 5, as_advance: true }],
    component_id_for_key: idForKey,
  });
  const withNonAdvance = buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot(),
    adjustments: [{ adjustment_id: ADJUSTMENT, pay_ex_vat: 5, as_advance: false }],
    component_id_for_key: idForKey,
  });
  const withNone = buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot(),
    component_id_for_key: idForKey,
  });
  assert.deepEqual(withAdvance, withNone);
  assert.deepEqual(withNonAdvance, withNone);
  assert.equal(withNone.some((row) => row.component.component_kind === 'ADJUSTMENT'), false);
});

test('binds configured source expenses once on the same ordinary Weekly root', () => {
  const result = buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot({
      expenses_pay_ex_vat: 30,
      expenses_charge_ex_vat: 30,
    }),
    expenses: [{
      authority_kind: 'SOURCE_EXPENSE',
      source_expense_id: '40000000-0000-4000-8000-000000000001',
      expense_code: 'SOURCE_SUPPLIED:40000000-0000-4000-8000-000000000011',
      pay_ex_vat: 12.5,
      charge_ex_vat: 12.5,
    }, {
      authority_kind: 'SOURCE_EXPENSE',
      source_expense_id: '40000000-0000-4000-8000-000000000002',
      expense_code: 'SOURCE_SUPPLIED:40000000-0000-4000-8000-000000000012',
      pay_ex_vat: 17.5,
      charge_ex_vat: 17.5,
    }],
    component_id_for_key: idForKey,
  });
  const expenses = result.filter((row) => row.component.component_kind === 'EXPENSE');
  assert.equal(expenses.length, 2);
  assert.equal(expenses.every((row) => row.authority.kind === 'SOURCE_EXPENSE'), true);
  assert.equal(expenses.reduce((sum, row) => sum + Number(row.component.pay_ex_vat), 0), 30);
  assert.equal(expenses.some((row) => row.component.expense_code === 'EXPENSES'), false);
});

test('refuses a source-expense manifest that does not own the exact TSFIN expense aggregate', () => {
  assert.throws(() => buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot({
      expenses_pay_ex_vat: 30,
      expenses_charge_ex_vat: 30,
    }),
    expenses: [{
      authority_kind: 'SOURCE_EXPENSE',
      source_expense_id: '40000000-0000-4000-8000-000000000001',
      expense_code: 'SOURCE_SUPPLIED:40000000-0000-4000-8000-000000000011',
      pay_ex_vat: 29,
      charge_ex_vat: 29,
    }],
    component_id_for_key: idForKey,
  }), { code: 'C1_EXPENSE_OWNERSHIP_CONFLICT' });
});

test('does not invent a component for a certified zero snapshot', () => {
  const result = buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot({
      additional_units_json: {},
      invoice_breakdown_json: { mode: 'SEGMENTS', segments: [] },
    }),
    component_id_for_key: idForKey,
  });
  assert.deepEqual(result, []);
});

test('publishes complete replacement entitlement values and never a locally calculated residual', () => {
  const componentsForPay = (payAmount) => buildWeeklySourceC1Components({
    root_timesheet_id: ROOT,
    financial_snapshot: snapshot({
      additional_units_json: {},
      invoice_breakdown_json: {
        mode: 'SEGMENTS',
        segments: [{
          segment_id: 'segment-1',
          segment_key: 'date:2026-09-14',
          segment_stable_key: 'event:one',
          date: '2026-09-14',
          ref_num: 'REF-1',
          hours_day: payAmount / 10,
          hours_night: 0,
          hours_sat: 0,
          hours_sun: 0,
          hours_bh: 0,
          pay_amount: payAmount,
          charge_amount: 200,
          exclude_from_pay: false,
        }],
      },
    }),
    component_id_for_key: idForKey,
  });

  const protectedTarget = componentsForPay(120);
  const reconciledTarget = componentsForPay(100);
  assert.equal(protectedTarget.length, 1);
  assert.equal(protectedTarget[0].component.pay_ex_vat, '120.00');
  assert.equal(reconciledTarget.length, 1);
  assert.equal(reconciledTarget[0].component.pay_ex_vat, '100.00');
  assert.notEqual(reconciledTarget[0].component.pay_ex_vat, '-20.00');
  assert.notEqual(reconciledTarget[0].component.pay_ex_vat, '20.00');
});
