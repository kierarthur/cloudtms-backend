import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const read = name => readFileSync(new URL(`../supabase/verification/${name}`, import.meta.url), 'utf8');

// Source-shape regression checks complement, and never replace, native
// populated-database rollback and managed NEW/UPGRADE evidence.
test('capture preserves existing rows and observes balanced mutations', () => {
  const s=read('support/22092026_1850_source_fixture_capture.sql');
  assert.match(s,/WS_VERIFY_EXISTING_ROW_MUTATION/);
  assert.match(s,/WS_VERIFY_TRUNCATE_FORBIDDEN/);
  assert.match(s,/inserts\+updates\+deletes into strict n/);
  assert.match(s,/inserts-deletes into strict n/);
  assert.match(s,/on commit drop/);
  assert.doesNotMatch(s,/to_jsonb\((?:new|old)\)/i);
});
test('capture reinclude does not reset accumulated evidence', () => {
  const s=read('support/22092026_1850_source_fixture_capture.sql');
  assert.equal((s.match(/create temporary table if not exists/g)||[]).length,2);
  assert.match(s,/if exists\(select 1 from pg_temp.ws_verify_relations where rel=p_rel\) then return/);
  assert.doesNotMatch(s,/truncate\s+(?:table\s+)?(?:pg_temp\.)?ws_verify_/i);
});
test('ordinary projection initializes included as well as standalone paths', () => {
  const s=read('15092026_1534_weekly_source_ordinary_pay_projection_v1.sql');
  const setup=s.indexOf('\\ir support/06102026_1117_source_workbench_fixture_isolation.sql');
  assert.ok(setup>=0);
  assert.match(read('support/06102026_1117_source_workbench_fixture_isolation.sql'),
    /\\ir 22092026_1850_source_fixture_capture\.sql/);
  assert.ok(s.indexOf('\\endif')<setup);
  assert.ok(setup<s.indexOf('\\ir 15092026_1534_weekly_source_finalisation_v1.sql'));
  assert.match(s,/billing_movement_count=\(\s*select pg_catalog\.count\(\*\) from public\.weekly_source_billing_movements/);
  assert.match(s,/and billing_movement_hash=\(/);
  assert.equal((s.match(/WEEKLY_SOURCE_ORDINARY_PROJECTION_MOVEMENT_BOUNDARY_V1/g)||[]).length,2);
});
test('fixture queue drains cannot retire unrelated queued jobs', () => {
  for(const n of ['17092026_0600_weekly_source_first_authorisation_v1.sql','17092026_0700_weekly_source_pending_entitlement_release_v1.sql','17092026_0300_weekly_source_entitlement_publication_v1.sql','17092026_1200_weekly_source_audit_and_export_v1.sql','17092026_0110_weekly_source_banking_pay_absence_v1.sql']) {
    const s=read(n);
    assert.match(s,/ws_verify_watch\('public.banking_pay_workbench_jobs'/,n);
    const drains=[...s.matchAll(/update public\.banking_pay_workbench_jobs\s+set status='SUCCEEDED'[\s\S]*?;/g)];
    assert.ok(drains.length>0,n);
    for(const [q] of drains) assert.match(q,/pg_temp.ws_verify_keys/,n);
  }
});
test('absence clear proves empty state rather than no prior writes', () => {
  const s=read('17092026_0110_weekly_source_banking_pay_absence_v1.sql');
  assert.match(s,/ws_verify_count\('private.weekly_source_banking_pay_absence'::regclass\)=0,\s*'clearing must leave the relation empty'/);
});
test('invoice stale-Draft proof moves a nonzero test-owned presentation', () => {
  const s=read('15092026_1534_weekly_source_invoice_admission_v1.sql');
  const proof=s.slice(s.indexOf('-- A Draft whose source allocation changed after the freeze'));
  assert.match(proof,/presentation.total_charge_ex_vat<>0 or presentation.vat_amount<>0/);
  assert.match(proof,/candidate.id in\(select \(key->>0\)::uuid from pg_temp.ws_verify_keys/);
  assert.match(proof,/source_binding.invoice_id in\(select \(key->>0\)::uuid from pg_temp.ws_verify_keys/);
  assert.match(proof,/v_frozen_source_revision is not null and v_frozen_destination_revision is not null/);
});
test('Source seam current-head lookups use the fixture root', () => {
  const s=read('17092026_0800_weekly_source_workbench_seams_v1.sql');
  for(const variable of ['v_head','v_zero']) {
    const m=s.match(new RegExp(`${variable} uuid:=\\(select id from public\\.weekly_source_entitlement_heads[\\s\\S]*?\\);`));
    assert.ok(m,variable);
    assert.match(m[0],/root_timesheet_id='17092026-0800-4000-8000-000000000005'/);
  }
});
