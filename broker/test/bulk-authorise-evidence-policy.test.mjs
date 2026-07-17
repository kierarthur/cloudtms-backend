import test from 'node:test';
import assert from 'node:assert/strict';
import {
  classifyBulkAuthoriseInvoiceEvidencePolicy,
  collectBulkAuthoriseInvoiceIds,
  missingBulkAuthoriseExpenseEvidenceKinds,
  normaliseBulkAuthoriseEvidenceKind,
  requiredBulkAuthoriseExpenseEvidenceKinds
} from '../src/bulk-authorise-evidence-policy.js';

test('collects whole and segment invoice locks without duplicates', () => {
  assert.deepEqual(collectBulkAuthoriseInvoiceIds({
    locked_by_invoice_id: 'whole',
    invoice_breakdown_json: { segments: [
      { invoice_locked_invoice_id: 'segment' },
      { invoice_locked_invoice_id: 'whole' },
      { locked_by_invoice_id: 'legacy-segment' }
    ] }
  }), ['whole', 'segment', 'legacy-segment']);
});

test('only unissued draft or on-hold invoices allow evidence changes', () => {
  assert.equal(classifyBulkAuthoriseInvoiceEvidencePolicy(['a', 'b'], [
    { id: 'a', status: 'DRAFT', issued_at_utc: null },
    { id: 'b', status: 'ON_HOLD', issued_at_utc: null }
  ]).allowed, true);

  const issued = classifyBulkAuthoriseInvoiceEvidencePolicy(['a'], [
    { id: 'a', status: 'DRAFT', issued_at_utc: '2026-07-01T00:00:00Z' }
  ]);
  assert.equal(issued.allowed, false);
  assert.deepEqual(issued.blocking_invoice_ids, ['a']);

  assert.equal(classifyBulkAuthoriseInvoiceEvidencePolicy(['missing'], []).allowed, false);
});

test('derives required evidence from positive matching financial values', () => {
  const financial = {
    mileage_units: 2,
    travel_charge_ex_vat: 5,
    accommodation_pay_ex_vat: 7,
    other_pay_ex_vat: 0,
    other_charge_ex_vat: 3
  };
  assert.deepEqual(requiredBulkAuthoriseExpenseEvidenceKinds(financial), [
    'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER'
  ]);
  assert.deepEqual(missingBulkAuthoriseExpenseEvidenceKinds(financial, [
    { kind: 'Miles' },
    { kind: 'Expenses' },
    { kind: 'Accommodation' }
  ]), ['OTHER']);
  assert.equal(normaliseBulkAuthoriseEvidenceKind('time sheet'), 'TIMESHEET');
});
