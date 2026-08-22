import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = process.cwd();
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
const migration = read('supabase/migrations/18082026_0802_candidate_daily_identity_integrity.sql');
const repeatable = read('supabase/repeatable/18082026_0131_candidate_daily_first_generation_source_link_v1.sql');
const master = read('docs/candidate-app/phase3-apps-script/master-rota/CloudTMSCandidateBridge.gs');
const runtime = read('tests/18082026_0138_candidate_daily_first_generation_source_link_runtime_verification.sql');
const integrityRuntime = read('tests/18082026_0807_candidate_daily_r16_identity_integrity_runtime_verification.sql');
const candidateWorkflow = read('.github/workflows/candidate-db-runtime.yml');
const migrateWorkflow = read('.github/workflows/supabase-migrate.yml');

test('R16 installs normalized active CID1 uniqueness after generic no-key preflight', () => {
  assert.match(migration, /upper\(btrim\(c\.key_norm\)\)[\s\S]*having count\(\*\)>1/i);
  assert.match(migration, /CANDIDATE_DAILY_CID1_NORMALIZED_DUPLICATE_PRECHECK_FAILED/);
  assert.doesNotMatch(migration, /raise\s+(?:notice|exception)[\s\S]{0,120}key_norm/i);
  assert.match(migration, /create unique index if not exists candidates_active_normalized_cid1_uq[\s\S]*upper\(btrim\(key_norm\)\)[\s\S]*active is true[\s\S]*\^CID1-/i);
});

test('R16 makes source HMAC ownership unique across all history and serializes every identity write', () => {
  assert.match(migration, /candidate_daily_source_links_history_hmac_uq[\s\S]*environment,source_system,hmac_key_version,identifier_hmac/i);
  assert.match(migration, /_candidate_daily_source_link_identity_history_guard_v1/);
  assert.match(migration, /pg_advisory_xact_lock[\s\S]*SOURCE:/i);
  assert.match(migration, /before insert or update of environment,source_system,hmac_key_version,identifier_hmac,candidate_id/i);
  assert.match(migration, /tg_op='UPDATE'[\s\S]*new\.candidate_id is distinct from old\.candidate_id[\s\S]*IDENTITY_LINK_CONFLICT/i);
  assert.match(migration, /IDENTITY_LINK_CONFLICT/);
  assert.doesNotMatch(repeatable, /where l\.state in \('PRIMARY','OVERLAP'\)[\s\S]{0,180}identifier_hmac=p_candidate_source_hmac/i);
  assert.match(repeatable, /v_candidate_link_history_count<>0[\s\S]*IDENTITY_LINK_CONFLICT/i);
  for (const state of ['retired-other', 'rejected-other', 'expired-other', 'future-other', 'retired-same']) {
    assert.match(integrityRuntime, new RegExp(state));
  }
});

test('R16 repeatable is atomic and closes private helper execution before defining the public RPC', () => {
  assert.match(repeatable, /^begin;\s*/i);
  assert.match(repeatable, /commit;\s*$/i);
  const helperEnd = repeatable.indexOf('$function$;', repeatable.indexOf('_candidate_daily_source_candidate_bind_on_generation_v1'));
  const immediateRevoke = repeatable.indexOf('revoke all on function private._candidate_daily_source_candidate_bind_on_generation_v1', helperEnd);
  const publicRpc = repeatable.indexOf('create or replace function public.candidate_daily_rota_generation_publish_atomic_v1');
  assert.ok(helperEnd >= 0 && immediateRevoke > helperEnd && publicRpc > immediateRevoke);
  for (const role of ['public', 'anon', 'authenticated', 'service_role']) {
    assert.match(repeatable.slice(helperEnd, publicRpc), new RegExp(`from ${role}`, 'i'));
  }
});

test('R16 Master treats the exact top-level identity conflict triple as terminal', () => {
  assert.match(master, /409:IDENTITY_LINK_CONFLICT:DO_NOT_RETRY/);
  assert.match(master, /ROTA_GENERATION_TERMINAL_REJECTION/);
  assert.match(master, /ROTA_GENERATION_MIRROR_COMPLETE/);
});

test('R16 dual-consumer runtime uses reconciliation and the real transition authority only', () => {
  assert.match(runtime, /candidate_daily_reconciliation_apply_atomic_v1/);
  assert.match(runtime, /candidate_daily_authority_transition_atomic_v1/);
  assert.match(runtime, /independent_approver_user_id/);
  assert.match(runtime, /Candidate read succeeded before the controlled authority transition/);
  assert.doesNotMatch(runtime, /insert into private\.candidate_daily_entitlements/i);
  assert.doesNotMatch(runtime, /update private\.candidate_daily_authority_scopes set authority_mode/i);
});

test('R16 safe migration cannot start before both exact Candidate PostgreSQL engines pass', () => {
  assert.match(candidateWorkflow, /workflow_call:/);
  assert.match(candidateWorkflow, /postgres:\s*\['17\.6',\s*'18\.1'\]/);
  assert.match(candidateWorkflow, /18082026_0802_candidate_daily_identity_integrity\.sql/);
  assert.match(candidateWorkflow, /18082026_0807_candidate_daily_r16_identity_integrity_runtime_verification\.sql/);
  assert.match(candidateWorkflow, /candidate-daily-r16-identity-integrity-concurrency\.integration\.js/);
  assert.match(migrateWorkflow, /candidate-db-runtime:[\s\S]*uses:\s*\.\/\.github\/workflows\/candidate-db-runtime\.yml/);
  assert.match(migrateWorkflow, /migrate:[\s\S]*needs:\s*candidate-db-runtime/);
  assert.match(migrateWorkflow, /Database source verification \(no deploy\)/);
  assert.match(migrateWorkflow, /npm run db:check/);
});
