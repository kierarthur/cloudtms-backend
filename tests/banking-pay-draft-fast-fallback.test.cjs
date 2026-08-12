const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const helpers = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '09082026_0712_banking_pay_semantic_ready_helpers.sql'
), 'utf8');
const expand = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1208_pay_payment_correction_expand_work.sql'
), 'utf8');

test('a non-untouched Draft rejects only the fast route before mutation and falls through safely', () => {
  const start = helpers.indexOf(
    'CREATE OR REPLACE FUNCTION private.pay_workbench_draft_overlay_remove_page_v1'
  );
  const end = helpers.indexOf(
    'ALTER FUNCTION private.pay_workbench_draft_overlay_remove_page_v1',
    start
  );
  const body = helpers.slice(start, end);
  const rejection = body.indexOf("'code','DRAFT_OVERLAY_FAST_NOT_UNTOUCHED'");
  const firstDraftMutation = body.indexOf('DROP TABLE IF EXISTS pg_temp._bpay_draft_overlay_candidate_page');

  assert.ok(start >= 0 && end > start);
  assert.ok(rejection >= 0 && rejection < firstDraftMutation);
  assert.match(body, /'fast_route_eligible',false/);
  assert.match(body, /'mutation_applied',false/);
  assert.doesNotMatch(body.slice(0, firstDraftMutation), /UPDATE public\.pay_batch_items/);
  assert.doesNotMatch(body.slice(0, firstDraftMutation), /UPDATE public\.pay_batches/);

  assert.match(expand, /IF COALESCE\(\(v_fast_draft_result->>'fast_route_eligible'\)::boolean,true\) IS NOT TRUE THEN/);
  assert.match(expand, /draft_overlay_fast_rejected/);
  assert.match(expand, /falls through to the existing frozen financial correction/);
  assert.match(expand, /v_work_kind := CASE WHEN v_request\.correction_kind = 'NO_MONEY_UNWIND'[\s\S]*ELSE 'PRE_BANK_CANCEL'/);
});
