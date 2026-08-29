import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = path => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const worker = read('broker/src/index.js');
const closure = read('supabase/repeatable/04082026_1151_pay_workbench_timesheet_dependency_closure_v2.sql');
const closureReassert = read('supabase/repeatable/29082026_1153_banking_pay_revoked_booking_dependency_v1.sql');
const emptyScope = read('supabase/repeatable/29082026_1153_banking_pay_empty_scope_reconciliation_v1.sql');
const syncAuthority = read('supabase/repeatable/04082026_1210_pay_sync_overpayments_from_preview.sql');

test('global drains never call the session repair owner without an exact session', () => {
  assert.match(worker, /if \(sourceBuildAllowedByFilter && sessionId\) \{\s+try \{\s+discardedSessionBlockerRepair = unwrapRpc\(await sbRpc\(env, 'pay_workbench_repair_discarded_session_blockers_v1'/);
  assert.doesNotMatch(worker, /if \(sourceBuildAllowedByFilter\) \{\s+try \{\s+discardedSessionBlockerRepair = unwrapRpc\(await sbRpc\(env, 'pay_workbench_repair_discarded_session_blockers_v1'/);
});

test('booking-family closure excludes withdrawn history but retains every economic identity fence', () => {
  const bookingFamily = closure.match(/ELSIF v_family=5 THEN[\s\S]*?ELSIF v_family IN \(6,7\) THEN/)?.[0] || '';
  const rotationFamily = closure.match(/ELSIF v_family IN \(6,7\) THEN[\s\S]*?ELSIF v_family IN \(8,9\) THEN/)?.[0] || '';
  assert.match(bookingFamily, /JOIN public\.timesheets AS related ON related\.booking_id=frontier\.booking_id/);
  assert.match(bookingFamily, /UPPER\(COALESCE\(related\.status::text,''\)\)<>'REVOKED'/);
  assert.match(rotationFamily, /JOIN public\.timesheets AS rotation_member\s+ON rotation_member\.timesheet_id=rotation\.family_timesheet_id/);
  assert.match(rotationFamily, /UPPER\(COALESCE\(rotation_member\.status::text,''\)\)<>'REVOKED'/);
  assert.match(closure, /PAY_WORKBENCH_DEPENDENCY_IDENTITY_CONFLICT/);
  assert.match(closure, /owner_row\.timesheet_id=v_row\.member_timesheet_id AND owner_row\.candidate_id=v_candidate_id/);
  assert.match(closureReassert, /\\ir 04082026_1151_pay_workbench_timesheet_dependency_closure_v2\.sql/);
});

test('empty authoritative scope uses one bounded no-finance reconciliation adapter', () => {
  assert.match(emptyScope, /CREATE OR REPLACE FUNCTION private\.pay_workbench_reconcile_empty_scope_v1/);
  for (const fence of [
    'scope_count<>0',
    'row_seal_count<>0',
    'dependency_node_count<>0',
    'dependency_closure_sealed_at_utc IS NULL',
    'dependency_edge_stream_complete IS NOT TRUE',
    'edge_tag_stream_complete IS NOT TRUE',
    "fact_family='EXPECTED_FINANCE_EFFECT'",
    'PAY_WORKBENCH_EMPTY_SCOPE_RECONCILIATION_INVALID',
  ]) assert.ok(emptyScope.includes(fence), `missing empty-scope fence: ${fence}`);

  assert.match(emptyScope, /status='RECONCILED',\s+private_stage='SOURCE_PUBLISH'/);
  assert.match(emptyScope, /canonical_count=0,\s+canonical_digest=v_canonical_digest/);
  assert.match(emptyScope, /'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'/);
  assert.match(emptyScope, /'empty_scope_reconciliation_version',1/);
  assert.match(emptyScope, /\\ir 04082026_1210_pay_sync_overpayments_from_preview\.sql/);
  assert.match(syncAuthority, /IF v_capture_mode THEN\s+RETURN v_result\|\|jsonb_build_object\(\s+'effect_plan_capture',true,\s+'captured_effects','\[\]'::jsonb/);
  assert.match(syncAuthority, /v_empty_finalize:=private\.pay_workbench_reconcile_empty_scope_v1/);
  assert.match(syncAuthority, /jsonb_typeof\(v_result\) IS DISTINCT FROM 'object'/);
  assert.match(syncAuthority, /jsonb_typeof\(v_result->'public_result_json'\) IS DISTINCT FROM 'object'/);
  assert.match(syncAuthority, /SET plpgsql_check\.mode TO 'disabled'/i);
});

test('the empty-scope adapter cannot write Draft, provider, payment or settlement authority', () => {
  for (const forbidden of [
    /(?:INSERT INTO|UPDATE|DELETE FROM)\s+public\.payment_batches/i,
    /(?:INSERT INTO|UPDATE|DELETE FROM)\s+public\.pay_batch_items/i,
    /(?:INSERT INTO|UPDATE|DELETE FROM)\s+public\.bank_payments/i,
    /(?:INSERT INTO|UPDATE|DELETE FROM)\s+public\.provider_/i,
    /(?:INSERT INTO|UPDATE|DELETE FROM)\s+public\.settlement/i,
    /(?:INSERT INTO|UPDATE|DELETE FROM)\s+public\.remittance/i,
    /CREATE\s+DRAFT/i,
  ]) assert.doesNotMatch(emptyScope, forbidden);
  assert.doesNotMatch(emptyScope + syncAuthority, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
