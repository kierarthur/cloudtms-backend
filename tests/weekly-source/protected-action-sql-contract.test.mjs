import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');

const owner = read('supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql');
const familyOwner = read('supabase/repeatable/15092026_1534_weekly_source_protected_pay_publisher_v1.sql');
const verifier = read('supabase/verification/15092026_1534_weekly_source_protected_action_orchestration_v1.sql');

const functionBody = (signature, nextSignature) => {
  const start = owner.indexOf(signature);
  assert.notEqual(start, -1, `${signature} is missing`);
  const end = nextSignature ? owner.indexOf(nextSignature, start + signature.length) : owner.length;
  assert.notEqual(end, -1, `${nextSignature} is missing`);
  return owner.slice(start, end);
};

test('all protected action RPCs are service-only and reload the API schema', () => {
  for (const name of [
    'weekly_exceptional_pay_prepare_action_v1',
    'weekly_exceptional_pay_action_context_v1',
    'weekly_exceptional_pay_action_publication_status_v1',
    'weekly_exceptional_pay_wait_atomic_v1',
  ]) {
    assert.match(owner, new RegExp(`revoke all on function public\\.${name}\\(jsonb\\)\\s+from public,anon,authenticated,service_role;`, 'i'));
    assert.match(owner, new RegExp(`grant execute on function public\\.${name}\\(jsonb\\)\\s+to service_role;`, 'i'));
  }
  assert.match(owner, /notify pgrst, 'reload schema';/i);
});

test('current source context excludes superseded Correct-final history', () => {
  const context = functionBody(
    'create or replace function public.weekly_exceptional_pay_action_context_v1',
    'create or replace function public.weekly_exceptional_pay_action_publication_status_v1',
  );
  assert.ok((context.match(/revision\.state='CURRENT'/g) || []).length >= 2);
  assert.match(context, /source_cycle\.source_group_id=v_group\.id/);
  assert.match(context, /with ranked as \(/);
  assert.match(context, /event_rank=1/);
  assert.match(context, /with ladder as \(/);
  assert.match(context, /private\.weekly_source_ordinary_projection_active_movements_v1\(/);
  assert.match(context, /movement\.invoice_timesheet_id=any\(v_root_family\)/);
  assert.doesNotMatch(context, /order by[\s\S]{0,160}created_at_utc[\s\S]{0,80}limit 1[\s\S]{0,80}weekly_source_final_revisions/i);
});

test('WAIT changes only protected lifecycle state and cannot write pay, invoice or Draft facts', () => {
  const waitOwner = functionBody(
    'create or replace function public.weekly_exceptional_pay_wait_atomic_v1',
    'alter function public.weekly_exceptional_pay_prepare_action_v1',
  );
  assert.match(waitOwner, /insert into public\.weekly_exceptional_pay_family_events/);
  assert.match(waitOwner, /update public\.weekly_exceptional_pay_target_families/);
  assert.match(waitOwner, /update public\.weekly_exceptional_orchestration_runs/);
  assert.doesNotMatch(waitOwner, /insert into public\.(timesheets_financials|invoice_lines|pay_batches)/i);
  assert.match(verifier, /WAIT must change only protected audit\/lifecycle state and no financial, invoice or Banking row/);
  assert.match(verifier, /WEEKLY_PROTECTED_ACTION_STALE/);
});

test('the rollback proof covers every protected Office action and exact replay', () => {
  for (const action of ['AMEND', 'WITHDRAW', 'WAIT', 'RECONCILE', 'RECORD_NOT_WORKED']) {
    assert.match(verifier, new RegExp(`['"]${action}['"]`));
  }
  assert.match(verifier, /WEEKLY_PROTECTED_ACTION_IDEMPOTENCY_COLLISION/);
  assert.match(verifier, /exact completed replay must survive the family version advance/);
  assert.match(verifier, /^rollback;$/m);
});

test('first approval and later actions serialize one shared idempotency namespace', () => {
  const lock = /pg_catalog\.pg_advisory_xact_lock\(pg_catalog\.hashtextextended\(\s*'weekly-exceptional-orchestration\|'\|\|v_(?:idempotency_key|key),0\s*\)\)/i;
  assert.match(familyOwner, lock);
  assert.match(owner, lock);
  assert.ok(
    familyOwner.indexOf('pg_catalog.pg_advisory_xact_lock')
      < familyOwner.indexOf('where run.idempotency_key=v_idempotency_key'),
  );
  assert.ok(
    owner.indexOf('pg_catalog.pg_advisory_xact_lock')
      < owner.indexOf('where run.idempotency_key=v_key'),
  );
});
