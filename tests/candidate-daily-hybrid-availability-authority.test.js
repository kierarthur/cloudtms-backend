import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = fs.readFileSync(path.join(
  root,
  'supabase/repeatable/12102026_2110_candidate_daily_legacy_two_way_availability_v1.sql'
), 'utf8');

test('signed legacy intent is accepted under both app authorities with existing edit guards', () => {
  assert.match(source, /authority_mode not in \('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY'\)/i);
  assert.match(source, /if v_reason is null then[\s\S]*v_accepted :=/i);
  assert.doesNotMatch(source, /v_reason\s*:=\s*'MYTMS_PRIMARY'/i);
  for (const guard of ['OUTSIDE_WINDOW', 'BOOKED', 'BLOCKED', 'NOT_EDITABLE', 'INVALID_VALUE', 'DUPLICATE_DATE']) {
    assert.ok(source.includes(`v_reason := '${guard}'`));
  }
  assert.match(source, /_candidate_daily_context_v1\(p_internal_context,'LEGACY_COMPAT',false\)/);
  assert.match(source, /where environment=v_environment and candidate_id=v_candidate_id\s+for update/);
});

test('MyTMS primary legacy edits project only accepted dates at the new canonical version', () => {
  assert.match(source, /candidate_daily_sheet_projection_outbox/i);
  assert.match(source, /if v_scope.authority_mode='SUPABASE_PRIMARY' then[\s\S]*jsonb_array_elements\(v_accepted\)/);
  assert.match(source, /v_change->>'date'\)::date,v_new_version/);
  assert.match(source, /state='LAGGING'/);
  assert.doesNotMatch(source, /'PENDING'/);
  assert.match(source, /candidate_daily_refresh_sync_state_v1/i);
  assert.doesNotMatch(source, /insert\s+into\s+control\.|update\s+control\./i);
  assert.match(source, /v_receipt.state<>'IN_PROGRESS'[\s\S]*_idempotent_replay/);
});

test('hybrid authority keeps service-only execution and the existing public signature', () => {
  assert.match(source, /create or replace function public\.candidate_daily_legacy_availability_apply_atomic_v1\(\s*p_internal_context jsonb,\s*p_candidate_source_hmac text,\s*p_request_id uuid,\s*p_idempotency_key text,\s*p_changes jsonb,\s*p_correlation_id text\s*\)/i);
  assert.match(source, /security definer\s+set search_path=''/i);
  assert.match(source, /revoke all on function[\s\S]*from public/i);
  assert.match(source, /from anon/i);
  assert.match(source, /from authenticated/i);
  assert.match(source, /grant execute on function[\s\S]*to service_role/i);
});
