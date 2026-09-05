import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const worker = readFileSync(
  new URL('../broker/src/invoice-async-http.js', import.meta.url),
  'utf8'
);
const sql = readFileSync(
  new URL('../supabase/repeatable/05092026_2053_unified_outbox_list_v2.sql', import.meta.url),
  'utf8'
);
const verifier = readFileSync(
  new URL('../supabase/verification/05092026_2109_unified_outbox_list_v2_verification.sql', import.meta.url),
  'utf8'
);
const release = JSON.parse(readFileSync(
  new URL('../supabase/release/current-release.json', import.meta.url),
  'utf8'
));

test('Outbox alternate sorts and membership share one unified RPC', () => {
  assert.match(worker, /path === '\/api\/summary-membership\/outbox'/);
  assert.match(worker, /handleUnifiedOutboxSummaryMembership\(env, req, deps\)/);
  assert.match(worker, /deps\.rpc\(\s*'outbox_unified_list_v2'/);
  assert.match(worker, /const selectionKey = `\$\{rowChannel\}::\$\{rowId\}`/);
  assert.match(worker, /OUTBOX_MEMBERSHIP_CARDINALITY_MISMATCH/);
  assert.match(worker, /sortBy === 'created_at_utc'[\s\S]*sortDir === 'desc'[\s\S]*cursorPayload\.v === 1/);
});

test('unified Outbox SQL sorts displayed values and includes every source', () => {
  assert.match(sql, /from public\.v_outbox_unified as u/i);
  assert.match(sql, /from public\.invoice_operations as o/i);
  assert.match(sql, /union all\s+select \* from invoice_source/i);
  assert.match(sql, /v_sort_by = 'status'[\s\S]*upper\(coalesce\(f\.status, ''\)\)/i);
  assert.doesNotMatch(sql, /v_sort_by = 'status'[\s\S]{0,120}f\.queue_state/i);
  assert.match(sql, /v_sort_by = 'channel'[\s\S]*f\.channel/i);
  assert.match(sql, /v_sort_by = 'scheduled_for_utc'[\s\S]*f\.scheduled_for_utc/i);
  assert.match(sql, /v_sort_by = 'effective_ready_at_utc'[\s\S]*f\.effective_ready_at_utc/i);
  assert.match(sql, /v_queue_state = 'READY' and b\.queue_state = 'READY'/i);
  assert.match(sql, /row_number\(\) over/i);
  assert.match(sql, /v_limit integer := least\(greatest\(coalesce\(p_limit, 50\), 1\), 500\)/i);
});

test('unified Outbox SQL remains service-only and refreshes PostgREST', () => {
  assert.match(sql, /security definer/i);
  assert.match(sql, /set search_path = pg_catalog, public, pg_temp/i);
  assert.match(sql, /revoke all on function public\.outbox_unified_list_v2[\s\S]*from public, anon, authenticated/i);
  assert.match(sql, /grant execute on function public\.outbox_unified_list_v2[\s\S]*to postgres, service_role/i);
  assert.match(sql, /notify pgrst, 'reload schema'/i);
});

test('database release runs the unified Outbox count, sort, and ACL verifier', () => {
  const verifierPath = 'supabase/verification/05092026_2109_unified_outbox_list_v2_verification.sql';
  assert.equal(release.verificationFiles.includes(verifierPath), true);
  assert.equal(release.newVerificationFiles.includes(verifierPath), true);
  assert.match(verifier, /v_expected_total := v_legacy_total \+ v_invoice_total/i);
  assert.match(verifier, /UNIFIED_OUTBOX_LIST_V2_SORT_ORDER_INVALID/i);
  assert.match(verifier, /has_function_privilege\('service_role', v_signature, 'EXECUTE'\)/i);
  assert.match(verifier, /rollback;/i);
});
