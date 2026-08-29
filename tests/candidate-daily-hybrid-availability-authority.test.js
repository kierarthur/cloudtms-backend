import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = fs.readFileSync(path.join(
  root,
  'supabase/repeatable/26082026_1059_candidate_daily_hybrid_availability_authority_v1.sql'
), 'utf8');

test('legacy availability preserves Google primary but cannot overwrite MyTMS primary', () => {
  assert.match(source, /authority_mode='SUPABASE_PRIMARY'[\s\S]*v_reason\s*:=\s*'MYTMS_PRIMARY'/i);
  assert.match(source, /authority_mode not in \('GOOGLE_PRIMARY','ROLLBACK_PENDING'\)/i);
  assert.match(source, /if v_reason is null then[\s\S]*v_accepted :=/i);
  assert.match(source, /if v_reason='MYTMS_PRIMARY' then[\s\S]*v_repairs :=/i);
});

test('MyTMS primary reassertion is exact Candidate/date projection work', () => {
  assert.match(source, /candidate_daily_sheet_projection_outbox/i);
  assert.match(source, /availability_date=\(v_change->>'date'\)::date/i);
  assert.match(source, /state=case[\s\S]*'TERMINAL'[\s\S]*else 'PENDING'/i);
  assert.match(source, /next_available_at_utc=now\(\)/i);
  assert.match(source, /candidate_daily_refresh_sync_state_v1/i);
  assert.doesNotMatch(source, /insert\s+into\s+control\.|update\s+control\./i);
});

test('hybrid authority keeps service-only execution and the existing public signature', () => {
  assert.match(source, /create or replace function public\.candidate_daily_legacy_availability_apply_atomic_v1\(\s*p_internal_context jsonb,\s*p_candidate_source_hmac text,\s*p_request_id uuid,\s*p_idempotency_key text,\s*p_changes jsonb,\s*p_correlation_id text\s*\)/i);
  assert.match(source, /security definer\s+set search_path=''/i);
  assert.match(source, /revoke all on function[\s\S]*from public/i);
  assert.match(source, /from anon/i);
  assert.match(source, /from authenticated/i);
  assert.match(source, /grant execute on function[\s\S]*to service_role/i);
});
