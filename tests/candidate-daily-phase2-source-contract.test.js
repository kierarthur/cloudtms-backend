import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = (path) => readFileSync(new URL(path, import.meta.url), 'utf8');

const schema = read('../supabase/migrations/17082026_0010_candidate_daily_phase2_authority_schema.sql');
const authority = read('../supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql');
const workflow = read('../.github/workflows/candidate-db-runtime.yml');

const expectedTables = [
  'private.candidate_daily_authority_scopes',
  'private.candidate_daily_entitlements',
  'private.candidate_daily_source_links',
  'public.candidate_daily_command_receipts',
  'private.candidate_daily_batch_receipts',
  'public.candidate_daily_rota_generations',
  'public.candidate_daily_rota_days',
  'public.candidate_daily_availability_days',
  'public.candidate_daily_sheet_projection_outbox',
  'private.candidate_daily_sync_state',
  'private.candidate_daily_authority_transitions',
  'private.candidate_daily_external_effect_receipts'
].sort();

const expectedRpcs = [
  'candidate_daily_legacy_availability_apply_atomic_v1',
  'candidate_daily_legacy_availability_status_get_v1',
  'candidate_daily_rota_generation_publish_atomic_v1',
  'candidate_daily_projection_claim_v1',
  'candidate_daily_projection_complete_atomic_v1',
  'candidate_daily_sync_status_get_v1',
  'candidate_daily_reconciliation_apply_atomic_v1',
  'candidate_daily_authority_transition_atomic_v1',
  'candidate_daily_external_effect_claim_v1',
  'candidate_daily_external_effect_complete_v1',
  'candidate_daily_external_effect_status_get_v1',
  'candidate_daily_tiles_get_v1',
  'candidate_daily_availability_apply_atomic_v1'
].sort();

test('Phase 2 installs exactly the approved twelve-table authority and no legacy browser session', () => {
  const tables = [...schema.matchAll(/create table if not exists\s+((?:public|private)\.candidate_daily_[a-z0-9_]+)/gi)]
    .map((match) => match[1].toLowerCase()).sort();
  assert.deepEqual(tables, expectedTables);
  assert.match(schema, /\|\|\s*'\{"candidate_daily_enabled":false\}'::jsonb/i);
  assert.doesNotMatch(schema, /candidate_daily_(?:browser|legacy)_sessions?/i);
  assert.doesNotMatch(schema, /\bmsisdn\b|\btelephone\b|\bmobile\b/i);
});

test('Phase 2 exposes exactly thirteen service RPCs and keeps all direct roles closed', () => {
  const rpcs = [...authority.matchAll(/create or replace function\s+public\.(candidate_daily_[a-z0-9_]+)\s*\(/gi)]
    .map((match) => match[1].toLowerCase()).sort();
  assert.deepEqual(rpcs, expectedRpcs);
  for (const name of expectedRpcs) {
    assert.match(authority, new RegExp(`revoke all on function public\\.${name}\\(`, 'i'));
  }
  assert.match(authority, /revoke all on function %s from anon/i);
  assert.match(authority, /revoke all on function %s from authenticated/i);
  assert.match(authority, /grant execute on function %s to service_role/i);
});

test('authority mode and freshness remain single-owner and fail closed', () => {
  assert.match(schema, /authority_mode\s+text\s+not null default 'GOOGLE_PRIMARY'/i);
  assert.match(schema, /check\s*\(\s*authority_mode\s+in\s*\(\s*'GOOGLE_PRIMARY'\s*,\s*'ROLLBACK_PENDING'\s*,\s*'SUPABASE_PRIMARY'\s*\)\s*\)/i);
  for (const owner of [
    'accepted_canonical_cursor', 'required_visible_cursor', 'delivered_visible_cursor',
    'overlay_proof_cursor', 'effective_visible_cursor'
  ]) assert.match(schema, new RegExp(`\\b${owner}\\b`, 'i'));
  assert.match(authority, /DEFERRED_OVERLAY[\s\S]*overlay_generation_id[\s\S]*overlay_source_row_hash/i);
  assert.match(authority, /effective_visible_cursor[\s\S]*required_visible_cursor/i);
});

test('Candidate runtime workflow installs Phase 2 schema then RPCs before dependent reads', () => {
  const schemaIndex = workflow.indexOf('supabase/migrations/17082026_0010_candidate_daily_phase2_authority_schema.sql');
  const rpcIndex = workflow.indexOf('supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql');
  const readIndex = workflow.indexOf('supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql');
  const testIndex = workflow.indexOf('tests/17082026_0053_candidate_daily_phase2_runtime_verification.sql');
  assert.ok(schemaIndex > 0 && rpcIndex > schemaIndex && readIndex > rpcIndex && testIndex > readIndex);
});
