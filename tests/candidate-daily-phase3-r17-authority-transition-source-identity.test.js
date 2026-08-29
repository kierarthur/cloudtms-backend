import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = process.cwd();
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
const authority = read('supabase/repeatable/18082026_1051_candidate_daily_authority_transition_source_identity_v1.sql');
const r16Guard = read('supabase/migrations/18082026_0802_candidate_daily_identity_integrity.sql');
const candidateWorkflow = read('.github/workflows/candidate-db-runtime.yml');
const migrateWorkflow = read('.github/workflows/supabase-migrate.yml');

test('R17 replaces exactly the existing authority-transition signature in one atomic repeatable', () => {
  assert.match(authority, /^begin;\s*/i);
  assert.match(authority, /commit;\s*$/i);
  assert.match(authority, /create or replace function public\.candidate_daily_authority_transition_atomic_v1\(\s*p_internal_context jsonb,\s*p_batch_request_id uuid,\s*p_idempotency_key text,\s*p_transition_items jsonb,\s*p_independent_approver uuid,\s*p_reason text,\s*p_evidence_sha256 text,\s*p_correlation_id text\s*\)/i);
  assert.match(authority, /security definer\s+set search_path=''\s+as \$function\$/i);
  assert.equal((authority.match(/create or replace function/gi) || []).length, 1);
});

test('R17 acquires every safe SOURCE lock before any Candidate scope row lock', () => {
  const sourceLoop = authority.indexOf('for v_source_lock_identity in');
  const sourceLock = authority.indexOf('pg_catalog.pg_advisory_xact_lock', sourceLoop);
  const scopeLock = authority.indexOf('from private.candidate_daily_authority_scopes s', sourceLock);
  assert.ok(sourceLoop >= 0 && sourceLock > sourceLoop && scopeLock > sourceLock);
  assert.match(authority.slice(sourceLoop, scopeLock), /select distinct[\s\S]*order by lock_identity/i);
  assert.match(authority.slice(sourceLoop, scopeLock), /v_environment\|\|':SOURCE:'\|\|\(x\.source_link->>'hmac_key_version'\)\|\|':'\|\|[\s\S]*identifier_hmac/i);
  assert.match(authority.slice(sourceLoop, scopeLock), /pg_catalog\.hashtextextended\(v_source_lock_identity,0\)/i);
  assert.match(r16Guard, /hashtextextended\([\s\S]*new\.environment\|\|':SOURCE:'\|\|new\.hmac_key_version::text\|\|':'\|\|new\.identifier_hmac/i);
});

test('R17 prelock filter cannot cast malformed source-link values before per-item validation', () => {
  const sourceLoop = authority.slice(
    authority.indexOf('for v_source_lock_identity in'),
    authority.indexOf('perform 1 from private.candidate_daily_authority_scopes s')
  );
  assert.match(sourceLoop, /jsonb_typeof\(x\.source_link\)='object'/i);
  assert.match(sourceLoop, /identifier_hmac' ~ '\^\[a-f0-9\]\{64\}\$'/i);
  assert.match(sourceLoop, /hmac_key_version' ~ '\^\[1-9\]\\d\*\$'/i);
  assert.match(sourceLoop, /length\(x\.source_link->>'hmac_key_version'\)<=10/i);
  assert.match(sourceLoop, /2147483647/);
  assert.doesNotMatch(sourceLoop, /::integer|::int\b/i);
  assert.match(authority, /raise exception using errcode='22023',message='VALIDATION_FAILED'/i);
});

test('R17 contains source identity conflicts as durable indexed item rejections', () => {
  assert.match(authority, /exception when others then[\s\S]*IDENTITY_LINK_CONFLICT[\s\S]*jsonb_build_object\('index',v_index,'status','REJECTED','error_code',v_error\)/i);
  assert.match(authority, /terminal_response_body=v_terminal/);
  assert.match(authority, /terminal_http_status=200/);
  assert.match(authority, /if v_batch\.state<>'IN_PROGRESS' then[\s\S]*_idempotent_replay/i);
});

test('R17 preserves closed ACLs and service-role-only execution', () => {
  assert.match(authority, /revoke all on function public\.candidate_daily_authority_transition_atomic_v1\([\s\S]*\) from public/i);
  assert.match(authority, /from anon/i);
  assert.match(authority, /from authenticated/i);
  assert.match(authority, /grant execute on function public\.candidate_daily_authority_transition_atomic_v1\([\s\S]*\) to service_role/i);
});

test('R17 PostgreSQL 17.6 and 18.1 gate installs and exercises the new authority before migration', () => {
  assert.match(candidateWorkflow, /postgres:\s*\['17\.6',\s*'18\.1'\]/);
  assert.match(candidateWorkflow, /18082026_1051_candidate_daily_authority_transition_source_identity_v1\.sql/);
  assert.match(candidateWorkflow, /18082026_1105_candidate_daily_r17_authority_transition_source_identity_runtime_verification\.sql/);
  assert.match(candidateWorkflow, /candidate-daily-r17-authority-transition-source-identity-concurrency\.integration\.js/);
  assert.match(migrateWorkflow, /candidate-db-runtime:[\s\S]*uses:\s*\.\/\.github\/workflows\/candidate-db-runtime\.yml/);
  assert.match(migrateWorkflow, /migrate:[\s\S]*needs:\s*candidate-db-runtime/);
});
