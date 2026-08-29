import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';

const repoRoot = path.resolve(import.meta.dirname, '../..');
const worker = fs.readFileSync(path.join(repoRoot, 'broker/src/index.js'), 'utf8');
const repeatable = fs.readFileSync(path.join(repoRoot, 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'), 'utf8');
const migration = fs.readFileSync(path.join(repoRoot, 'supabase/migrations/20260718223641_fix_banking_alert_diagnostic_context.sql'), 'utf8');

function sliceBetween(source, start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  assert.notEqual(from, -1, `missing start marker: ${start}`);
  assert.notEqual(to, -1, `missing end marker: ${end}`);
  return source.slice(from, to);
}

const cancelHandler = sliceBetween(
  worker,
  'async function handleBankingPayBatchCancel',
  'async function revolutPayment_create'
);

test('whole-draft cancellation makes the frozen item scope authoritative', () => {
  assert.match(cancelHandler, /const cancellationScopeType = upperText/);
  assert.match(cancelHandler, /selectionJson\.work_unit = \['BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH'\]\.includes\(cancellationScopeType\)/);
  assert.match(cancelHandler, /\? 'BATCH'\s*: \(cancellationScopeType === 'TRANSFER' \? 'TRANSFER' : 'CANDIDATE'\)/);
  assert.match(cancelHandler, /Policy X: the database diagnostic resolves the frozen post-draft item IDs/);
  assert.match(cancelHandler, /p_selection_json: selectionJson/);
});

test('database cancellation path independently preserves the same frozen scope rule', () => {
  const cancelFunction = sliceBetween(
    repeatable,
    'CREATE OR REPLACE FUNCTION public.pay_payment_cancel_not_sent_and_recalculate',
    'CREATE OR REPLACE FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refresh'
  );
  assert.match(cancelFunction, /'scope_type', v_scope_type,\s*'work_unit', CASE/);
  assert.match(cancelFunction, /WHEN v_scope_type IN \('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH'\) THEN 'BATCH'/);
  assert.match(cancelFunction, /'expected_pay_batch_item_ids', COALESCE\(v_resolved_scope_json -> 'pay_batch_item_ids'/);
});

test('selection drift is returned as a specific safe conflict without leaking frozen item IDs', () => {
  const helpers = sliceBetween(cancelHandler, '  const extractDbRaisedJson =', '  const fetchWorkbenchSessionRow =');
  const context = {
    env: {},
    req: {},
    JSON,
    Number,
    Math,
    Object,
    Array,
    String,
    safeObject(value) { return value && typeof value === 'object' && !Array.isArray(value) ? value : {}; },
    trimText(value) { return String(value == null ? '' : value).trim(); },
    upperText(value) { return String(value == null ? '' : value).trim().toUpperCase(); },
    toArray(value) { return Array.isArray(value) ? value : []; },
    sanitizePaymentIssueDisplayMessage(_value, fallback) { return fallback; },
    jsonResponse(status, payload) { return { status, payload }; },
    withCORS(_env, _req, response) { return response; }
  };
  vm.runInNewContext(`${helpers}\nglobalThis.respond = rpcErrorResponse;`, context, { filename: 'cancel-error-response.js' });

  const response = context.respond({
    json: {
      code: 'P0001',
      message: 'WORK_SELECTION_DRIFT',
      details: JSON.stringify({
        code: 'WORK_SELECTION_DRIFT',
        expected_pay_batch_item_ids: Array.from({ length: 12 }, (_, index) => `item-${index + 1}`),
        resolved_pay_batch_item_ids: ['item-1'],
        scope_type: 'BATCH'
      })
    }
  }, 'CANCEL_FAILED', 'Cancellation failed.');

  assert.equal(response.status, 409);
  assert.equal(response.payload.error_code, 'WORK_SELECTION_DRIFT');
  assert.equal(response.payload.code, 'WORK_SELECTION_DRIFT');
  assert.match(response.payload.user_message, /12 frozen payment items were expected but 1 was selected internally/);
  assert.match(response.payload.user_message, /Nothing was cancelled/);
  assert.deepEqual(JSON.parse(JSON.stringify(response.payload.details)), {
    scope_type: 'BATCH',
    expected_item_count: 12,
    resolved_item_count: 1
  });
  assert.equal(JSON.stringify(response.payload).includes('item-1'), false);
});

test('alert computation supplies both mandatory diagnostic contexts', () => {
  const alertFunction = sliceBetween(
    repeatable,
    'CREATE OR REPLACE FUNCTION public.banking_alerts_active_for_user',
    'CREATE OR REPLACE FUNCTION public.banking_alerts_refresh_for_user'
  );
  assert.match(alertFunction, /p_provider_diagnostic_context := 'PAYMENT_ISSUES_PROVIDER_DIAGNOSTIC'/);
  assert.match(alertFunction, /p_actor_user_id,\s*'PAYMENT_ISSUES_TAB'\s*\)/);
});

test('migration is guarded, idempotent and patches only the two exact functions', () => {
  assert.match(migration, /pg_get_function_identity_arguments/);
  assert.match(migration, /BANKING_ALERT_PROVIDER_DIAGNOSTIC_CALL_PATCH_COUNT_INVALID/);
  assert.match(migration, /BANKING_ALERT_CANCELABILITY_CALL_PATCH_COUNT_INVALID/);
  assert.match(migration, /PAYMENT_CANCEL_FROZEN_SCOPE_PATCH_COUNT_INVALID/);
  assert.match(migration, /p_provider_diagnostic_context := 'PAYMENT_ISSUES_PROVIDER_DIAGNOSTIC'/);
  assert.match(migration, /'PAYMENT_ISSUES_TAB'/);
  assert.match(migration, /EXECUTE v_definition/g);
  assert.equal((migration.match(/EXECUTE v_definition/g) || []).length, 2);
  assert.doesNotMatch(migration, /\b(?:INSERT|UPDATE|DELETE|TRUNCATE)\b/i);
  assert.doesNotMatch(migration, /GRANT\s+EXECUTE/i);
});
