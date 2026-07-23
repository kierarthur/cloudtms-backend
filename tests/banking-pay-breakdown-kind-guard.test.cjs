const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repeatable = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);
const guard = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/23072026_1354_pay_batch_breakdown_kind_guard.sql'),
  'utf8'
);
const migration = fs.readFileSync(
  path.resolve(__dirname, '../supabase/migrations/23072026_1354_pay_batch_breakdown_kind_guard.sql'),
  'utf8'
);

const financeKinds = [
  'OVERPAYMENT_RECOVERY',
  'LOAN_REPAYMENT',
  'MANUAL_DEBT_RECOVERY',
  'MANUAL_CREDIT_PAYOUT',
  'LOAN_PAYOUT',
  'UNDERPAYMENT_PAYMENT',
  'DEBT_CREATED'
];

test('draft breakdown builder preserves finance item kinds instead of labelling them as expenses', () => {
  const start = repeatable.indexOf('), fallback_breakdown_rows AS (');
  const end = repeatable.indexOf('FROM pg_temp.tmp_pay_batch_breakdown_item_page AS item_page', start);
  assert.ok(start >= 0 && end > start, 'fallback breakdown builder must exist');
  const body = repeatable.slice(start, end);

  for (const kind of financeKinds) assert.match(body, new RegExp(`'${kind}'`));
  assert.match(body, /THEN item_page\.item_type/);
});

test('database guard enforces the same frozen finance breakdown kinds', () => {
  for (const source of [guard, migration]) {
    assert.match(source, /_pay_batch_item_breakdown_kind_guard_v1/);
    assert.match(source, /NEW\.line_kind := v_item_type/);
    for (const kind of financeKinds) assert.match(source, new RegExp(`'${kind}'`));
  }
  assert.match(migration, /BEFORE INSERT OR UPDATE OF pay_batch_item_id, line_kind/);
});
