import assert from 'node:assert/strict';
import test from 'node:test';

import { isCertifiedDeferredFinanceOnlyInsertItemsResult } from '../broker/src/banking-pay-draft-insert-items.js';

const payBatchId = '11111111-1111-4111-8111-111111111111';
const operationId = '22222222-2222-4222-8222-222222222222';
const candidateScopeIds = [
  '33333333-3333-4333-8333-333333333333',
  '44444444-4444-4444-8444-444444444444'
];

const certifiedResult = (overrides = {}) => ({
  ok: true,
  pay_batch_id: payBatchId,
  operation_id: operationId,
  candidate_scope_count: 2,
  candidate_scope_ids: [...candidateScopeIds].reverse(),
  allocation_row_count: 9,
  page_allocation_row_count: 9,
  ordinary_page_allocation_row_count: 0,
  deferred_finance_adjustment_rows: 9,
  linked_allocation_rows: 0,
  repaired_existing_item_links: 0,
  inserted_item_rows: 0,
  reused_item_rows: 0,
  skipped_item_rows: 9,
  failed_item_rows: 0,
  has_more: false,
  ...overrides
});

const expected = { payBatchId, operationId, candidateScopeIds };

test('accepts the database-certified recovery-only INSERT_ITEMS no-op', () => {
  assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(certifiedResult(), expected), true);
});

test('requires exact operation, batch and candidate-scope identity', () => {
  assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(certifiedResult({ pay_batch_id: operationId }), expected), false);
  assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(certifiedResult({ operation_id: payBatchId }), expected), false);
  assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(certifiedResult({ candidate_scope_count: 1 }), expected), false);
  assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(certifiedResult({ candidate_scope_ids: [candidateScopeIds[0]] }), expected), false);
  assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(certifiedResult({ candidate_scope_ids: [candidateScopeIds[0], candidateScopeIds[0]] }), expected), false);
});

test('rejects ordinary, partial, failed and continuing item work', () => {
  const invalidResults = [
    { ordinary_page_allocation_row_count: 1 },
    { deferred_finance_adjustment_rows: 0, page_allocation_row_count: 0, skipped_item_rows: 0 },
    { page_allocation_row_count: 8 },
    { skipped_item_rows: 8 },
    { linked_allocation_rows: 1 },
    { repaired_existing_item_links: 1 },
    { inserted_item_rows: 1 },
    { reused_item_rows: 1 },
    { failed_item_rows: 1 },
    { ok: false },
    { has_more: true }
  ];
  for (const overrides of invalidResults) {
    assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(certifiedResult(overrides), expected), false);
  }
});

test('rejects malformed or missing proof fields instead of guessing', () => {
  for (const result of [null, [], {}, certifiedResult({ deferred_finance_adjustment_rows: 'not-a-number' }), certifiedResult({ skipped_item_rows: -1 })]) {
    assert.equal(isCertifiedDeferredFinanceOnlyInsertItemsResult(result, expected), false);
  }
});
