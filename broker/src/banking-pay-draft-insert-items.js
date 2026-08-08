const text = (value) => String(value == null ? '' : value).trim();

const canonicalId = (value) => text(value).toLowerCase();

const nonNegativeInteger = (value) => {
  const numberValue = Number(value);
  return Number.isSafeInteger(numberValue) && numberValue >= 0
    ? numberValue
    : null;
};

const canonicalUniqueIds = (values) => {
  if (!Array.isArray(values)) return null;
  const canonical = values.map(canonicalId);
  if (canonical.some((value) => !value)) return null;
  const unique = Array.from(new Set(canonical));
  if (unique.length !== canonical.length) return null;
  return unique.sort();
};

const equalIdSets = (left, right) => {
  const leftIds = canonicalUniqueIds(left);
  const rightIds = canonicalUniqueIds(right);
  if (!leftIds || !rightIds || leftIds.length !== rightIds.length) return false;
  return leftIds.every((value, index) => value === rightIds[index]);
};

export function isCertifiedDeferredFinanceOnlyInsertItemsResult(resultLike, expected = {}) {
  const result = resultLike && typeof resultLike === 'object' && !Array.isArray(resultLike)
    ? resultLike
    : {};
  const expectedScopeIds = Array.isArray(expected.candidateScopeIds)
    ? expected.candidateScopeIds
    : [];

  const candidateScopeCount = nonNegativeInteger(result.candidate_scope_count);
  const allocationRowCount = nonNegativeInteger(result.allocation_row_count);
  const pageAllocationRowCount = nonNegativeInteger(result.page_allocation_row_count);
  const ordinaryPageAllocationRowCount = nonNegativeInteger(result.ordinary_page_allocation_row_count);
  const deferredFinanceAdjustmentRows = nonNegativeInteger(result.deferred_finance_adjustment_rows);
  const linkedAllocationRows = nonNegativeInteger(result.linked_allocation_rows);
  const repairedExistingItemLinks = nonNegativeInteger(result.repaired_existing_item_links);
  const insertedItemRows = nonNegativeInteger(result.inserted_item_rows);
  const reusedItemRows = nonNegativeInteger(result.reused_item_rows);
  const skippedItemRows = nonNegativeInteger(result.skipped_item_rows);
  const failedItemRows = nonNegativeInteger(result.failed_item_rows);

  return result.ok === true
    && result.has_more === false
    && canonicalId(result.pay_batch_id) === canonicalId(expected.payBatchId)
    && canonicalId(result.operation_id) === canonicalId(expected.operationId)
    && expectedScopeIds.length > 0
    && candidateScopeCount === expectedScopeIds.length
    && equalIdSets(result.candidate_scope_ids, expectedScopeIds)
    && ordinaryPageAllocationRowCount === 0
    && deferredFinanceAdjustmentRows !== null
    && deferredFinanceAdjustmentRows > 0
    && pageAllocationRowCount === deferredFinanceAdjustmentRows
    && allocationRowCount !== null
    && allocationRowCount >= deferredFinanceAdjustmentRows
    && skippedItemRows === deferredFinanceAdjustmentRows
    && linkedAllocationRows === 0
    && repairedExistingItemLinks === 0
    && insertedItemRows === 0
    && reusedItemRows === 0
    && failedItemRows === 0;
}
