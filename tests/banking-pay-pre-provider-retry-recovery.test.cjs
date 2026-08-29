const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const source = fs.readFileSync(path.join(__dirname, '..', 'broker', 'src', 'index.js'), 'utf8');

function bodyBetween(startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `missing ${startMarker}`);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.notEqual(end, -1, `missing ${endMarker}`);
  return source.slice(start, end);
}

test('a safely cleaned pre-provider execution failure remains terminal and retryable', () => {
  const body = bodyBetween(
    'async function advanceBankingPayExecuteOperation',
    'async function withBankingPayOperationLease'
  );
  const cleanupIndex = body.indexOf("failedLocalArtifactCleanup = await rpc('pay_execute_operation_cleanup_failed_local_artifacts'");
  const retrySafeIndex = body.indexOf('const cleanupMadeRetrySafe = !releaseFailure');
  const returnIndex = body.indexOf("code: 'PAYMENT_EXECUTE_PRE_PROVIDER_FAILURE_RETRYABLE'");
  const reviewIndex = body.lastIndexOf('return reviewRequired(currentPhase, reviewCode');

  assert.ok(cleanupIndex >= 0);
  assert.ok(retrySafeIndex > cleanupIndex);
  assert.ok(returnIndex > retrySafeIndex);
  assert.ok(reviewIndex > returnIndex);
  assert.match(body, /cleanupResult\.safe_to_retry === true/);
  assert.match(body, /cleanupResult\.retry_blocked === false/);
  assert.match(body, /cleanupResult\.review_required === false/);
  assert.match(body, /requires_user_action: false/);
  assert.match(body, /review_required: false/);
  assert.match(body, /safe_to_retry: true/);
});

test('explicit payment retry revalidates the exact batch operation before cleanup', () => {
  const body = bodyBetween(
    'async function handleBankingPayBatchExecutePayment',
    'async function handleBankingPayBatchExportCsv'
  );

  assert.match(body, /retry_pre_provider_operation_id/);
  assert.match(body, /banking_pay_operations\?id=eq\.\$\{encodeURIComponent\(retryPreProviderOperationId\)\}&pay_batch_id=eq\.\$\{encodeURIComponent\(id\)\}&operation_type=eq\.PAYMENT_EXECUTE&status=eq\.REVIEW_REQUIRED/);
  assert.match(body, /const retryablePreProviderFailureCodes = new Set\(\[/);
  assert.match(body, /'PAYMENT_EXECUTE_OPERATION_FAILED'/);
  assert.match(body, /'BATCH_STALE'/);
  assert.match(body, /!retryablePreProviderFailureCodes\.has\(priorFailureCode\)/);
  assert.match(body, /code: priorFailureCode/);
  assert.match(body, /pay_execute_operation_cleanup_failed_local_artifacts/);
  assert.match(body, /cleanupBatchId === id/);
  assert.match(body, /cleanupOperationId === retryPreProviderOperationId/);
  assert.match(body, /retryPreProviderCleanup\.safe_to_retry === true/);
  assert.match(body, /retryPreProviderCleanup\.retry_blocked === false/);
  assert.match(body, /retryPreProviderCleanup\.review_required === false/);
  assert.match(body, /PAYMENT_EXECUTION_RETRY_REQUIRES_REVIEW/);
});

