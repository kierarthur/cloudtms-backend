import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const single = await readFile(path.join(
  repoRoot,
  'supabase/repeatable/21072026_1235_30_timesheet_unauthorise_atomic.sql',
), 'utf8');
const bulk = await readFile(path.join(
  repoRoot,
  'supabase/repeatable/21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql',
), 'utf8');
const broker = await readFile(path.join(repoRoot, 'broker/src/index.js'), 'utf8');

test('single ordinary unauthorise locks the rotation family before its invoice census', () => {
  const trimmedLock = single.indexOf('pg_advisory_xact_lock(hashtext(BTRIM(v_requested_booking_id)))');
  const rawLock = single.indexOf('pg_advisory_xact_lock(hashtext(v_requested_booking_id))');
  const familyRowLock = single.indexOf('WHERE family_ts.booking_id = v_requested_booking_id');
  const familyFinancialLock = single.indexOf('WHERE family_tf.timesheet_id = ANY(v_family_timesheet_ids)');
  const mutation = single.indexOf('UPDATE public.timesheets AS ts');

  assert.ok(trimmedLock >= 0);
  assert.ok(rawLock > trimmedLock);
  assert.ok(familyRowLock > rawLock);
  assert.ok(familyFinancialLock > familyRowLock);
  assert.ok(mutation > familyFinancialLock);
  assert.match(single, /v_requested_ts\.booking_id IS DISTINCT FROM v_requested_booking_id/);
});

test('single ordinary unauthorise checks every physical family member', () => {
  assert.match(single, /family_tf\.timesheet_id = ANY\(v_family_timesheet_ids\)[\s\S]*family_tf\.locked_by_invoice_id IS NOT NULL/);
  assert.match(single, /family_tf\.timesheet_id = ANY\(v_family_timesheet_ids\)[\s\S]*invoice_locked_invoice_id/);
  assert.match(single, /invoice_line\.timesheet_id = ANY\(v_family_timesheet_ids\)/);
  assert.doesNotMatch(single, /invoice_line\.timesheet_id = v_current_ts\.timesheet_id/);
  assert.match(single, /COALESCE\(v_has_family_tsfin_invoice_lock, false\)[\s\S]*COALESCE\(v_has_segment_invoice_lock, false\)[\s\S]*COALESCE\(v_has_invoice_membership, false\)/);
});

test('bulk ordinary unauthorise snapshots and locks families in deterministic order', () => {
  const snapshot = bulk.indexOf('CREATE TEMP TABLE timesheet_unauthorise_bulk_family_snapshots');
  const order = bulk.indexOf('ORDER BY BTRIM(family_snapshot.booking_id_snapshot), family_snapshot.booking_id_snapshot');
  const trimmedLock = bulk.indexOf('pg_advisory_xact_lock(hashtext(v_family.trimmed_booking_id))');
  const rawLock = bulk.indexOf('pg_advisory_xact_lock(hashtext(v_family.booking_id))');
  const familyRowLock = bulk.indexOf('WHERE family_ts.booking_id = v_family.booking_id');
  const familyFinancialLock = bulk.indexOf('FOR UPDATE OF family_tf;');
  const mutation = bulk.indexOf('UPDATE public.timesheets AS ts_upd');

  assert.ok(snapshot >= 0);
  assert.ok(order > snapshot);
  assert.ok(trimmedLock > order);
  assert.ok(rawLock > trimmedLock);
  assert.ok(familyRowLock > rawLock);
  assert.ok(familyFinancialLock > familyRowLock);
  assert.ok(mutation > familyFinancialLock);
  assert.match(bulk, /requested_booking_id IS DISTINCT FROM state_rows\.requested_booking_id_snapshot/);
});

test('bulk ordinary unauthorise refuses every family invoice-lock representation', () => {
  assert.match(bulk, /has_family_tsfin_invoice_lock/);
  assert.match(bulk, /has_family_segment_invoice_lock/);
  assert.match(bulk, /has_family_invoice_membership/);
  assert.match(bulk, /OR state_rows\.has_family_tsfin_invoice_lock[\s\S]*OR state_rows\.has_family_segment_invoice_lock[\s\S]*OR state_rows\.has_family_invoice_membership THEN 'TIMESHEET_LOCKED_BY_INVOICE'/);
});

test('ordinary unauthorise remains Worker-owned and is never directly browser executable', () => {
  assert.match(single, /REVOKE ALL ON FUNCTION public\.timesheet_unauthorise_atomic\([^)]+\) FROM PUBLIC, anon, authenticated, service_role;/);
  assert.match(single, /GRANT EXECUTE ON FUNCTION public\.timesheet_unauthorise_atomic\([^)]+\) TO service_role;/);
  assert.doesNotMatch(single, /GRANT EXECUTE ON FUNCTION public\.timesheet_unauthorise_atomic\([^)]+\) TO authenticated/);

  assert.match(bulk, /REVOKE ALL ON FUNCTION public\.timesheet_unauthorise_bulk_atomic\([^)]+\) FROM PUBLIC, anon, authenticated, service_role;/);
  assert.match(bulk, /GRANT EXECUTE ON FUNCTION public\.timesheet_unauthorise_bulk_atomic\([^)]+\) TO service_role;/);
  assert.doesNotMatch(bulk, /GRANT EXECUTE ON FUNCTION public\.timesheet_unauthorise_bulk_atomic\([^)]+\) TO authenticated/);

  assert.match(broker, /function sbHeaders\(env\) \{[\s\S]{0,300}const key = env\.SUPABASE_SERVICE_ROLE_KEY;[\s\S]{0,300}["']Authorization["']:\s*`Bearer \$\{key\}`/);
  assert.match(broker, /callTimesheetLifecycleRpcWithTransientRetry\(env, 'timesheet_unauthorise_atomic'/);
  assert.match(broker, /'timesheet_unauthorise_bulk_atomic'/);
});
