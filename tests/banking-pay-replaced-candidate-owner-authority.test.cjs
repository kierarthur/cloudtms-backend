const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

test('replacement-session candidate work is re-established only through the canonical refresh owner', () => {
  const sql = read('supabase/repeatable/29082026_0613_banking_pay_replaced_candidate_owner_repair_v1.sql');

  assert.match(sql, /CREATE OR REPLACE FUNCTION public\.pay_workbench_session_replay_replaced_queue_v1\(/i);
  assert.match(sql, /public\.pay_workbench_enqueue_candidate_refresh\([\s\S]*REPLACED_SESSION_CANONICAL_CANDIDATE_REFRESH/i);
  assert.match(sql, /'canonical_refresh_from_replaced_session',true/i);
  assert.match(sql, /'force_legacy',true,'force_broad_legacy',true/i);
  assert.match(sql, /WHERE replay_job\.candidate_id IS NULL[\s\S]*WORKBENCH_SESSION_SCOPE_SEED/i);
  assert.doesNotMatch(sql, /WHERE replay_job\.candidate_id IS NOT NULL[\s\S]{0,300}INSERT INTO public\.banking_pay_workbench_jobs/i);
  assert.match(sql, /PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SCOPE_OWNER_MISSING/i);
  assert.match(sql, /source_build_run_id/i);
  assert.match(sql, /source_change_seq/i);
  assert.match(sql, /session_version/i);
});

test('persisted raw replay jobs are terminalised under candidate lock and rebound through existing owner repair', () => {
  const sql = read('supabase/repeatable/29082026_0613_banking_pay_replaced_candidate_owner_repair_v1.sql');

  assert.match(sql, /CREATE OR REPLACE FUNCTION public\.pay_workbench_repair_replayed_candidate_jobs_v1\(/i);
  assert.match(sql, /pg_try_advisory_xact_lock[\s\S]*_pay_workbench_candidate_serial_key/i);
  assert.match(sql, /REPLAY_REPLACED_SESSION:[\s\S]*source_job:%/i);
  assert.match(sql, /status='DEAD'[\s\S]*REPLACED_SESSION_CANDIDATE_JOB_REQUIRES_CANONICAL_OWNER/i);
  assert.match(sql, /public\.pay_workbench_repair_orphaned_pending_source_build\(/i);
  assert.match(sql, /PAY_WORKBENCH_REPLAYED_CANDIDATE_OWNER_REPAIR_POSTCONDITION_FAILED/i);
  assert.match(sql, /EXCEPTION WHEN OTHERS[\s\S]*REPAIR_FAILED_NO_PARTIAL_ADOPTION/i);
  assert.match(sql, /REVOKE ALL ON FUNCTION public\.pay_workbench_repair_replayed_candidate_jobs_v1\(uuid,uuid,integer,text\) FROM PUBLIC,anon,authenticated,service_role;/i);
  assert.match(sql, /NOTIFY pgrst, 'reload schema';/i);
});

test('drain repairs replayed candidate owners before any source-build claim and fails closed', () => {
  const source = read('broker/src/index.js');
  const start = source.indexOf("logDrainDiag('WORKBENCH_DRAIN_START'");
  const repair = source.indexOf("sbRpc(env, 'pay_workbench_repair_replayed_candidate_jobs_v1'", start);
  const firstClaim = source.indexOf("pay_workbench_source_build_attempt_claim_start_v1", start);

  assert.ok(start >= 0, 'drain start marker must exist');
  assert.ok(repair > start, 'replay-owner repair must follow drain initialization');
  assert.ok(firstClaim > repair, 'replay-owner repair must settle before any source-build claim');
  assert.match(source.slice(repair, firstClaim), /WORKBENCH_REPLAYED_CANDIDATE_OWNER_REPAIR_FAILED[\s\S]*throw repairError;/i);
});

test('rollback verifier proves canonical replay, persisted repair, deferral and idempotency', () => {
  const verification = read('supabase/verification/29082026_0614_banking_pay_replaced_candidate_owner_verification.sql');

  assert.match(verification, /^BEGIN;/m);
  assert.match(verification, /ROLLBACK;/m);
  assert.match(verification, /BANKING_PAY_REPLACED_CANDIDATE_RAW_JOB_COPIED/i);
  assert.match(verification, /BANKING_PAY_REPLAYED_CANDIDATE_REPAIR_OWNER_INVALID/i);
  assert.match(verification, /BANKING_PAY_REPLACED_CANDIDATE_DEFER_CONTRACT_INVALID/i);
  assert.match(verification, /BANKING_PAY_REPLAYED_CANDIDATE_REPAIR_NOT_IDEMPOTENT/i);
  assert.doesNotMatch(verification, /CREATE\s+DRAFT|provider submission|settlement execution/i);
});
