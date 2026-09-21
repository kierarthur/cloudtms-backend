import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');
const executableSql = source => source
  .replace(/--[^\n]*/g, '')
  .replace(/\/\*[\s\S]*?\*\//g, '');

const migration = read('supabase/migrations/15092026_2335_weekly_source_finalisation_pay_orchestration.sql');
const owner = read('supabase/repeatable/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql');
const verifier = read('supabase/verification/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql');

const RPCS = Object.freeze([
  'weekly_source_finalisation_pay_open_atomic_v1',
  'weekly_source_finalisation_pay_task_start_atomic_v1',
  'weekly_source_finalisation_pay_task_finish_atomic_v1',
  'weekly_source_finalisation_pay_task_unknown_atomic_v1',
  'weekly_source_finalisation_pay_task_recover_atomic_v1',
]);

test('migration adds only server-owned durable run and task checkpoints with RLS', () => {
  assert.match(migration, /create table public\.weekly_source_finalisation_pay_runs/i);
  assert.match(migration, /create table public\.weekly_source_finalisation_pay_tasks/i);
  assert.match(migration, /final_revision_id uuid not null unique[\s\S]*references public\.weekly_source_final_revisions/i);
  assert.match(migration, /projection_receipt_id uuid unique[\s\S]*references public\.weekly_source_ordinary_pay_projection_receipts/i);
  // Plan 6.2 Gate 2 / S9: the task state mirrors the projection receipt outcome,
  // and both lost 'PUBLISHED' and 'REFUSED_LOCKED'.  The projection no longer
  // publishes an entitlement, and a paid, invoiced or Draft-frozen root is no
  // longer refused because the later path no longer mutates it.
  assert.match(migration, /'READY','SUBMISSION_STARTED','RECOVERY_REQUIRED','PREPARED_FOR_AUTHORISATION',[\s\S]*'PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED','FAILED'/i);
  // No enumerated state is REFUSED_LOCKED any more (the only surviving mention
  // is the comment that records why it went).
  assert.doesNotMatch(migration, /'REFUSED_LOCKED',/i);
  // WB-016: the task carries the Timesheet family identity, and the run-level
  // uniqueness is on the family, not on one physical Timesheet id.
  assert.match(migration, /root_family_booking_id text not null/i);
  assert.match(migration, /root_timesheet_version integer not null check \(root_timesheet_version>=1\)/i);
  assert.match(migration, /create unique index weekly_source_finalisation_pay_tasks_run_family_uq[\s\S]*run_id,pg_catalog\.btrim\(root_family_booking_id\)/i);
  // The physical-id key is gone from the table body (the only surviving mention
  // is the comment that records why it went).
  assert.doesNotMatch(migration, /unique \(run_id,root_timesheet_id\),/i);
  assert.match(migration, /alter table public\.weekly_source_finalisation_pay_runs enable row level security/i);
  assert.match(migration, /alter table public\.weekly_source_finalisation_pay_tasks enable row level security/i);
  assert.match(migration, /revoke all on table public\.weekly_source_finalisation_pay_runs[\s\S]*from public,anon,authenticated,service_role/i);
  assert.match(migration, /revoke all on table public\.weekly_source_finalisation_pay_tasks[\s\S]*from public,anon,authenticated,service_role/i);
});

test('all orchestration RPCs are exact-json service-only owners', () => {
  for (const rpc of RPCS) {
    assert.match(owner, new RegExp(`create or replace function public\\.${rpc}\\(\\s*p_request jsonb\\s*\\)`, 'i'));
    assert.match(owner, new RegExp(`revoke all on function public\\.${rpc}\\(jsonb\\)[\\s\\S]*?from public,anon,authenticated,service_role`, 'i'));
    assert.match(owner, new RegExp(`grant execute on function public\\.${rpc}\\(jsonb\\)[\\s\\S]*?to service_role`, 'i'));
  }
  assert.equal((owner.match(/WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED/g) || []).length, RPCS.length);
  assert.match(owner, /jsonb_object_keys\(p_request\)/i);
  assert.match(owner, /notify pgrst, 'reload schema';/i);
});

test('source finalisation manifest is immutable and each root gets one stable projection identity', () => {
  assert.match(owner, /revision\.reason<>'INITIAL_FINALISATION'/i);
  assert.match(owner, /revision\.state<>'CURRENT'/i);
  assert.match(owner, /weekly_source_finalisation_pay_context_v1\(\s*v_revision\.id,roots\.root_timesheet_id/i);
  assert.match(migration, /unique \(run_id,root_timesheet_id\)/i);
  assert.match(owner, /'weekly-source-finalisation-pay:'\|\|v_revision\.id::text\|\|':'\|\|[\s\S]*root_timesheet_id/i);
  assert.match(owner, /prepared_context_hash is distinct from v_context_hash/i);
  assert.match(owner, /entry\.value-\(array\['prepared_context_hash'\]::text\[\]\)/i);
});

test('unknown outcomes stop durably and only explicit receipt-first recovery can re-arm', () => {
  const unknown = owner.indexOf('create or replace function public.weekly_source_finalisation_pay_task_unknown_atomic_v1');
  const recover = owner.indexOf('create or replace function public.weekly_source_finalisation_pay_task_recover_atomic_v1');
  assert.ok(unknown >= 0 && recover > unknown);
  const unknownBody = owner.slice(unknown, recover);
  const recoverBody = owner.slice(recover);
  assert.match(unknownBody, /set state='RECOVERY_REQUIRED'/i);
  assert.doesNotMatch(unknownBody, /weekly_source_ordinary_pay_projection_apply_atomic_v1/i);
  assert.match(recoverBody, /where receipt\.idempotency_key=v_task\.projection_idempotency_key/i);
  assert.match(recoverBody, /if found then/i);
  assert.match(recoverBody, /if not v_confirm then/i);
  assert.match(recoverBody, /set state='READY'/i);
  assert.doesNotMatch(recoverBody, /weekly_source_ordinary_pay_projection_apply_atomic_v1/i);
  assert.match(verifier, /a repeated START was permitted to become a hidden retry/i);
  assert.match(verifier, /receipt-first recovery permitted an unconfirmed retry/i);
  assert.match(verifier, /original projection idempotency key/i);
});

test('finish trusts only the independently stored ordinary projection receipt', () => {
  assert.match(owner, /from public\.weekly_source_ordinary_pay_projection_receipts receipt[\s\S]*receipt\.id=v_receipt_id[\s\S]*receipt\.idempotency_key=v_task\.projection_idempotency_key/i);
  assert.match(owner, /v_receipt\.receipt_hash is distinct from v_receipt_hash/i);
  assert.match(owner, /v_receipt\.final_revision_id is distinct from v_run\.final_revision_id/i);
  assert.match(owner, /v_receipt\.root_timesheet_id is distinct from v_task\.root_timesheet_id/i);
  assert.match(owner, /set state=v_receipt\.outcome,projection_receipt_id=v_receipt\.id/i);
  assert.match(verifier, /join public\.weekly_source_billing_movements transition_movement[\s\S]*transition_movement\.invoice_timesheet_id=movement\.invoice_timesheet_id[\s\S]*transition_row\.ordinary_source_entitlement_projection_state='PENDING'/i);
  // Plan 6.2 Gate 2 / S9: there is no REFUSED_LOCKED receipt to checkpoint as
  // action-required any more.  The verifier now proves the terminal state the
  // projection really returns for an already-authorised root.
  assert.match(verifier, /a receipt-led PROPOSED finish must checkpoint a terminal, non-action-required task/i);
});

test('database orchestration never invokes or mutates invoice, Workbench, Draft or Banking Pay owners', () => {
  const sql = executableSql(`${migration}\n${owner}`);
  assert.doesNotMatch(sql, /weekly_source_ordinary_pay_projection_apply_atomic_v1/i);
  assert.doesNotMatch(sql, /\binvoice_lines\b/i);
  assert.doesNotMatch(sql, /\binvoices\b/i);
  assert.doesNotMatch(sql, /\bpay_batches\b/i);
  assert.doesNotMatch(sql, /\bpay_batch_items\b/i);
  assert.doesNotMatch(sql, /\bpay_workbench\b/i);
  assert.doesNotMatch(sql, /\bbanking_pay\b/i);
  assert.match(verifier, /finalisation-pay checkpoints changed invoice or Banking Pay-owned rows/i);
});
