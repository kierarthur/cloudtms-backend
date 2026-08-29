const assert = require('node:assert/strict');
const test = require('node:test');
const oracle = require('./fixtures/banking-pay-legacy-oracle.cjs')();
const cases = require('./fixtures/banking-pay-modal-amount-cases.cjs');
const selectable = (row, siblings) => oracle.isPreviewRowSelectionAllowed(row)
  && !oracle.isReadyTimesheetDisplayContextLine(row) && !oracle.isSyntheticTimesheetResidualLine(row, siblings);
for (const fixture of cases) {
  test(`${fixture.id}: legacy display — ${fixture.label}`, () => {
    const selected = fixture.rows.filter(row => row.selected && selectable(row, fixture.rows));
    const amount = selected.reduce((sum, row) => sum + Number(oracle.getPreviewLineDisplayAmount(row)), 0);
    assert.equal(amount.toFixed(2), fixture.amount);
    assert.ok(!Object.is(amount, -0));
    const unselected = fixture.rows.map(row => ({ ...row, selected: false, selection_state: 'UNSELECTED' }));
    assert.equal(unselected.filter(row => row.selected && selectable(row, unselected)).length, 0);
  });
}
test('deduction indicator uses selected canonical marker, not a negative sign or unselected debt', () => {
  const payment = { ...cases.find(item => item.id === 'AMT-006').rows[0], amount_display: '-25.00' };
  const debt = cases.find(item => item.id === 'AMT-010').rows[0];
  assert.equal([payment].some(row => row.selected && row.is_recognised_finance_deduction), false);
  assert.equal([{ ...debt, selected: false }].some(row => row.selected && row.is_recognised_finance_deduction), false);
  assert.equal([payment, debt].some(row => row.selected && row.is_recognised_finance_deduction), true);
});
