const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const guard = read('supabase/repeatable/04082026_1142_pay_payment_mutation_guard_v1.sql');
const processChunk = read('supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql');
const statusPage = read('supabase/repeatable/04082026_1146_pay_batch_payment_status_page_v1.sql');

test('a financially finalised correction retains its gate only for unfinished grouped Workbench refresh', () => {
  assert.match(guard, /refresh_operation\.phase\s*=\s*'REFRESH_WORKBENCH'/);
  assert.match(guard, /refresh_operation\.status\s+NOT IN\s*\('COMPLETE',\s*'FAILED',\s*'CANCELLED'\)/);
  assert.match(guard, /refresh_operation\.input_json\s*->>\s*'correction_request_id'\s*=\s*request_row\.id::text/);
  assert.match(processChunk, /v_phase\s*=\s*'REFRESH_WORKBENCH'/);
  assert.match(processChunk, /pay_workbench_enqueue_candidate_refresh_many\s*\(/);
  assert.match(processChunk, /LIMIT 100/);
});

test('the post-financial refresh phase stages Workbench work and does not repeat economic application', () => {
  const start = processChunk.indexOf("IF v_phase = 'REFRESH_WORKBENCH' THEN");
  const end = processChunk.indexOf("IF v_phase = 'COMPLETE'", start);
  assert.ok(start > 0 && end > start);
  const body = processChunk.slice(start, end);
  assert.match(body, /pay_workbench_enqueue_candidate_refresh_many\s*\(/);
  assert.match(body, /pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1\s*\(/);
  assert.match(body, /v_requested_action\s+IN\s*\('DRAFT_CANCEL',\s*'PRE_BANK_CANCEL',\s*'NO_MONEY_UNWIND'\)/);
  assert.match(body, /v_session_id,\s*v_request\.pay_batch_id,\s*'DRAFT_CANCEL'/);
  assert.match(body, /'changed_pay_batch_item_ids',\s*v_refresh_pay_batch_item_ids/);
  assert.match(body, /public\.pay_payment_correction_items\s+AS\s+correction_item/);
  assert.match(body, /PAYMENT_CORRECTION_WORKBENCH_OVERLAY_RESTORE_RETRY/);
  assert.match(body, /workbench_refresh_status/);
  assert.doesNotMatch(body, /pay_pre_bank_cancel_apply_work_item\s*\(/);
  assert.doesNotMatch(body, /pay_no_money_unwind_apply_work_item\s*\(/);
  assert.doesNotMatch(body, /UPDATE\s+public\.pay_batch_items/i);
  assert.doesNotMatch(body, /UPDATE\s+public\.pay_bank_transfers/i);
});

test('cancelled amount presentation is sourced from immutable correction evidence', () => {
  assert.match(statusPage, /latest_removal_request AS/);
  assert.match(statusPage, /public\.pay_payment_correction_items/);
  assert.match(statusPage, /public\.pay_payment_correction_request_candidates/);
  assert.match(statusPage, /removed_frozen_source_amount/);
  assert.match(statusPage, /removed_frozen_payable_amount/);
  assert.match(statusPage, /removed_reviewed_payment_amount/);
  assert.match(statusPage, /'cancelled_gross_base_amount_pence'/);
  assert.match(statusPage, /'cancelled_payable_amount_pence'/);
  assert.match(statusPage, /'cancelled_bank_amount_pence'/);
  assert.doesNotMatch(statusPage, /live_finance_component/i);
});
