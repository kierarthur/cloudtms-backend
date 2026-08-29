import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const read = (path) => fs.readFileSync(new URL(`../${path}`, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const replacement = 'supabase/repeatable/29082026_0115_candidate_daily_projection_expired_lease_recovery_v1.sql';
const verifier = 'supabase/verification/29082026_0116_candidate_daily_projection_expired_lease_recovery_verification.sql';
const runtimeProof = 'tests/29082026_0117_candidate_daily_projection_expired_lease_recovery_runtime_verification.sql';
const sql = read(replacement);

test('a new projection claim recovers only a bounded set of expired leases', () => {
  assert.match(sql, /state='CLAIMED'[\s\S]*lease_expires_at_utc<=now\(\)/);
  assert.match(sql, /for update skip locked[\s\S]*limit p_max_items/i);
  assert.match(sql, /delivery_attempt_count=o\.delivery_attempt_count\+1/);
  assert.match(sql, /delivery_attempt_count\+1>=12 then 'TERMINAL' else 'RETRY'/);
  assert.match(sql, /safe_error_code='LEASE_EXPIRED'/);
  assert.match(sql, /lease_owner=null,lease_token=null,lease_expires_at_utc=null/);
  assert.match(sql, /make_interval\(secs=>power\(2,least\(o\.delivery_attempt_count\+1,16\)\)::integer\)/);
});

test('active leases and immutable completed claim receipts keep their existing guards', () => {
  assert.match(sql, /lease_expires_at_utc>now\(\)[\s\S]*message='LEASE_CONFLICT'/);
  assert.match(sql, /v_batch\.state<>'IN_PROGRESS'[\s\S]*message='LEASE_EXPIRED_STATUS_REQUIRED'/);
  assert.match(sql, /return v_batch\.terminal_response_body\|\|jsonb_build_object\('_idempotent_replay',true\)/);
  assert.doesNotMatch(sql, /pg_catalog\.(coalesce|nullif|least|greatest)\s*\(/i);
});

test('the replacement retains the exact operation and closed ACL boundary', () => {
  assert.equal((sql.match(/create or replace function/gi) || []).length, 1);
  assert.match(sql, /public\.candidate_daily_projection_claim_v1\(\s*p_internal_context jsonb,\s*p_claim_request_id uuid,\s*p_idempotency_key text,\s*p_target text,\s*p_claimant text,\s*p_max_items integer default 50,\s*p_lease_seconds integer default 120,\s*p_correlation_id text default null\s*\)/);
  assert.match(sql, /revoke all on function public\.candidate_daily_projection_claim_v1[\s\S]*from public/);
  assert.match(sql, /revoke all on function %s from anon/);
  assert.match(sql, /revoke all on function %s from authenticated/);
  assert.match(sql, /grant execute on function %s to service_role/);
  assert.doesNotMatch(sql, /\b(create table|alter table|drop table|create schema|alter schema|drop schema)\b/i);
});

test('UPGRADE and NEW both require static and rollback-contained real first-use proof', () => {
  const release = JSON.parse(read('supabase/release/current-release.json'));
  for (const key of ['verificationFiles', 'newVerificationFiles']) {
    assert.ok(release[key].includes(verifier), `${key} must include the lease recovery verifier`);
  }
  const verification = read(verifier);
  assert.match(verification, /FOR UPDATE SKIP LOCKED/);
  assert.match(verification, /LIMIT P_MAX_ITEMS/);
  assert.match(verification, /browser execution exposed/);
  const runtime = read(runtimeProof);
  for (const marker of ['expired row was not safely retried', 'exhausted row was not terminal',
    'active lease was changed', 'recovered retry did not receive a fresh full lease',
    'delayed old completion was accepted', 'current completion was not accepted']) {
    assert.ok(runtime.includes(marker), marker);
  }
  assert.match(runtime, /^begin;/m);
  assert.match(runtime, /^rollback;\s*$/m);
});
