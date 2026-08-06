const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const statusPage = read('supabase/repeatable/04082026_1146_pay_batch_payment_status_page_v1.sql');
const selectionPrepare = read('supabase/repeatable/04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql');
const requestStart = read('supabase/repeatable/04082026_1207_pay_payment_correction_request_start.sql');
const progressStatus = read('supabase/repeatable/04082026_1145_pay_payment_correction_status_get_v1.sql');
const monolith = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
const worker = read('broker/src/index.js');

function functionBody(name) {
  const markers = [`function ${name}`, `async function ${name}`];
  const start = markers.map((marker) => worker.indexOf(marker)).filter((value) => value >= 0).sort((a, b) => a - b)[0];
  assert.ok(Number.isInteger(start) && start >= 0, `${name} missing`);
  const boundaries = [worker.indexOf('\nfunction ', start + 10), worker.indexOf('\nasync function ', start + 10)].filter((value) => value > start);
  return worker.slice(start, boundaries.length ? Math.min(...boundaries) : worker.length);
}

test('terminal batch lifecycle is denied by status, planning, preparation and diagnostic', () => {
  const terminalPattern = /'COMMITTED', 'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'/;
  assert.match(statusPage, terminalPattern);
  assert.match(statusPage, /WHEN v_batch_terminal THEN ARRAY\[\]::text\[\]/);
  assert.match(selectionPrepare, terminalPattern);
  assert.match(selectionPrepare, /PAYMENT_CORRECTION_BATCH_TERMINAL/);
  assert.ok((requestStart.match(/PAYMENT_CORRECTION_BATCH_TERMINAL/g) || []).length >= 4);
  assert.match(monolith, /v_batch_terminal boolean := false/);
  assert.match(monolith, /'code', 'PAYMENT_CORRECTION_BATCH_TERMINAL'/);
});

test('central mutable scope excludes retained voided frozen history', () => {
  const resolverStart = monolith.indexOf('CREATE OR REPLACE FUNCTION public._pay_resolve_payment_scope_for_cancel_rewind');
  assert.ok(resolverStart >= 0);
  const resolver = monolith.slice(resolverStart, monolith.indexOf('\n$function$;', resolverStart) + 12);
  assert.match(resolver, /COALESCE\(candidate_item_rows\.is_voided, false\) IS NOT TRUE/);
  assert.match(resolver, /expanded_items AS/);
  assert.match(resolver, /'pay_batch_item_ids'/);
  assert.match(statusPage, /COALESCE\(shared_scope_item\.is_voided, false\) IS NOT TRUE/);
});

test('cursor authority is based on the actual limit-plus-one row', () => {
  assert.match(statusPage, /LIMIT p_limit \+ 1/);
  assert.match(statusPage, /SELECT pg_catalog\.count\(\*\) > p_limit\s+FROM paged/);
  assert.match(statusPage, /IF v_last_row IS NOT NULL AND v_has_more THEN/);
  assert.match(statusPage, /'has_more', v_has_more/);
  assert.doesNotMatch(statusPage, /v_row_count = p_limit THEN/);

  const pages = Array.from({ length: 100 }, (_, index) => ({
    rows: Array.from({ length: 100 }, (__, offset) => index * 100 + offset),
    hasMore: index < 99,
  }));
  assert.equal(pages.flatMap((page) => page.rows).length, 10000);
  assert.equal(pages.at(-1).hasMore, false);
});

test('resolution context exposes no operation or transfer identity', () => {
  const rowContextStart = statusPage.indexOf("'resolution_context', CASE");
  const rowContextEnd = statusPage.indexOf("'stable_sort_cursor'", rowContextStart);
  const rowContext = statusPage.slice(rowContextStart, rowContextEnd);
  assert.match(rowContext, /'candidate_token'/);
  assert.match(rowContext, /'active_batch_scope_hash'/);
  assert.match(rowContext, /'context_token'/);
  assert.doesNotMatch(rowContext, /operation_id|pay_bank_transfer_id|instruction_scope_ids/);
});

test('Worker revalidates and derives exact resolution authority before one event ingest', () => {
  const body = functionBody('handleBankingPayPaymentStatusResolveV1');
  assert.match(body, /PAYMENT_STATUS_RESOLUTION_CLIENT_AUTHORITY_PROHIBITED/);
  assert.match(body, /pay_batch_payment_status_page_v1/);
  assert.match(body, /currentActions\.includes\('RESOLVE_PAYMENT_STATUS'\)/);
  assert.match(body, /pay_payment_cancelability_diagnostic/);
  assert.match(body, /diagnostic\?\.requires_bank_check !== true/);
  assert.match(body, /banking_pay_operation_transfer_scope\?/);
  assert.match(body, /banking_pay_operation_transfer_scope_items\?/);
  assert.match(body, /transferKey === expectedTransferKey && itemKey === expectedItemKey/);
  assert.equal((body.match(/pay_bank_event_ingest/g) || []).length, 2);
  assert.equal((body.match(/sbRpc\(/g) || []).length, 3);
});

test('failed-payment release has server-owned processing wording', () => {
  assert.match(progressStatus, /v_request_kind = 'RELEASE_FAILED_PAYMENT'/);
  assert.match(progressStatus, /Releasing failed payments/);
  assert.match(progressStatus, /releasing the selected failed payments/);
});
