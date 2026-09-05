'use strict';
const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const read = name => fs.readFileSync(path.join(root, name), 'utf8').replace(/\r\n/g, '\n');
const coreFile = 'supabase/repeatable/28082026_1424_banking_pay_modal_candidate_selection_core.sql';
const bridgeFile = 'supabase/repeatable/28082026_1424_banking_pay_modal_selection_owner_bridge.sql';
const finalOwnerFile = 'supabase/repeatable/04092026_2355_banking_pay_workbench_selection_owner_reassert_v1.sql';
const oldFile = 'supabase/repeatable/09082026_1727_pay_workbench_session_set_selected_rows_semantic_overlay.sql';
function body(source) {
  const start = source.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_selected_rows(');
  assert.notEqual(start, -1);
  const open = source.indexOf('AS $function$\n', start) + 'AS $function$\n'.length;
  const close = source.indexOf('$function$;', open);
  assert.ok(close > open);
  return source.slice(open, close);
}
test('candidate selection is dispatched by the unchanged existing owner body', () => {
  const bridge = body(read(bridgeFile));
  const match = bridge.match(/  -- BEGIN BANKING_PAY_MODAL_CANDIDATE_DISPATCH_V2\n[\s\S]*?  -- END BANKING_PAY_MODAL_CANDIDATE_DISPATCH_V2\n/);
  assert.ok(match, 'exact candidate dispatch branch is required');
  assert.equal(bridge.replace(match[0], ''), body(read(oldFile)), 'every old request branch must remain unchanged');
  assert.match(match[0], /private\.pay_workbench_modal_candidate_selection_apply_v2/);
});
test('candidate core is private, has a fixed search path and reuses recovery once', () => {
  const sql = read(coreFile);
  assert.match(sql, /CREATE OR REPLACE FUNCTION private\.pay_workbench_modal_candidate_selection_apply_v2/);
  assert.match(sql, /SECURITY INVOKER SET search_path TO ''/);
  assert.equal((sql.match(/public\.pay_workbench_revalidate_zero_retained_recovery_headroom_v1\(/g) || []).length, 1);
  assert.match(sql, /private\.pay_workbench_modal_ready_members_v2/);
  assert.match(sql, /private\.pay_workbench_modal_selection_rows_v2/);
  assert.match(sql, /FOR UPDATE/);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(sql, /\b(?:sum|round|abs)\s*\(|amount_ex_vat\s*[+*\/-]|\b(?:min|max)\s*\([^)]*::uuid/i);
  assert.doesNotMatch(sql, /\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\.(?:pay_batches|pay_batch_candidates|pay_bank_transfers|pay_finance_cases|finance_components)\b/i);
});
test('candidate core records one final revision/audit and exact idempotent/no-op outcomes', () => {
  const sql = read(coreFile);
  assert.equal((sql.match(/public\._audit_insert\(/g) || []).length, 1);
  assert.equal((sql.match(/progress_counter_version\s*=\s*v_session\.progress_counter_version\s*\+\s*1/g) || []).length, 1);
  assert.match(sql, /BANKING_PAY_V2_REQUEST_CONFLICT/);
  assert.match(sql, /BANKING_PAY_V2_STALE_REVISION/);
  assert.match(sql, /candidate_selection_receipt_v2/);
  assert.match(sql, /'state_changed',false/);
  assert.match(sql, /'SELECT_ALL_READY'/);
  assert.match(sql, /'CLEAR_ALL_READY'/);
  assert.ok(sql.indexOf('CREATE TEMPORARY TABLE pg_temp._bpay_modal_candidate_final_ready_v2')
    > sql.indexOf('v_revalidation:=public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1('),
  'final eligible membership must be read after recovery revalidation');
});
test('selection bridge adds no browser-role grant and retains explicit schema reload', () => {
  const sql = read(bridgeFile);
  assert.match(sql, /REVOKE ALL[\s\S]*FROM PUBLIC, anon, authenticated, service_role/);
  assert.doesNotMatch(sql, /GRANT[^;]*\bTO\s+[^;]*\b(?:anon|authenticated|PUBLIC)\b/i);
  assert.match(sql, /notify pgrst, 'reload schema'/i);
});
test('catalogue-safe final owner is byte-equivalent and contains no include or transaction wrapper', () => {
  const bridge = read(bridgeFile);
  const finalOwner = read(finalOwnerFile);
  assert.equal(body(finalOwner), body(bridge));
  assert.doesNotMatch(finalOwner, /^\s*\\i[ r]?\s+/mi);
  assert.doesNotMatch(finalOwner, /^\s*(?:begin|commit|rollback|start\s+transaction)\s*;/mi);
  assert.match(finalOwner, /ALTER FUNCTION public\.pay_workbench_session_set_selected_rows\(uuid,jsonb,uuid\) OWNER TO postgres/);
  assert.match(finalOwner, /REVOKE ALL[\s\S]*FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(finalOwner, /GRANT EXECUTE[\s\S]*TO postgres, service_role/);
});
