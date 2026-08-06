import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = relativePath => readFileSync(new URL(relativePath, repoRoot), 'utf8');

const claim = read('supabase/repeatable/04082026_1141_pay_workbench_source_build_attempt_claim_start_v1.sql');
const execute = read('supabase/repeatable/04082026_1143_pay_workbench_source_build_attempt_execute_v1.sql');
const attempts = read('supabase/migrations/04082026_1134_banking_pay_bounded_scope_v12.sql');
const complete = read('supabase/repeatable/04082026_1219_pay_workbench_complete_job.sql');
const fail = read('supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql');
const recovery = read('supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql');
const oldDrain = read('supabase/repeatable/04082026_1219_pay_workbench_worker_drain_chunk.sql');

test('RPC 1 is service-role-only metadata claim/start with a concrete durable attempt', () => {
  assert.match(claim, /CREATE OR REPLACE FUNCTION public\.pay_workbench_source_build_attempt_claim_start_v1\(/i);
  assert.match(claim, /SECURITY DEFINER[\s\S]*SET search_path = ''/i);
  assert.match(claim, /REVOKE ALL ON FUNCTION[\s\S]*FROM PUBLIC,anon,authenticated/i);
  assert.match(claim, /GRANT EXECUTE ON FUNCTION[\s\S]*TO service_role/i);
  assert.match(claim, /INSERT INTO private\.banking_pay_workbench_economic_builds/i);
  assert.match(claim, /INSERT INTO private\.banking_pay_workbench_stage_attempts/i);
  assert.match(claim, /attempt_status[\s\S]*'STARTED'/i);
  assert.match(claim, /'attempt_nonce',v_attempt_nonce/i);
  assert.match(claim, /v_scan_limit integer:=50/i);
  assert.match(claim, /WITH claim_source AS MATERIALIZED[\s\S]*LIMIT v_scan_limit/i);
  assert.match(claim, /FOR UPDATE SKIP LOCKED;[\s\S]*'result_code','LANE_SCAN_BUSY'/i);
  const advisoryBusyBranch = claim.match(
    /IF NOT pg_catalog\.pg_try_advisory_xact_lock[\s\S]*?THEN([\s\S]*?)END IF;/i,
  );
  assert.ok(advisoryBusyBranch);
  assert.match(advisoryBusyBranch[1], /CONTINUE/i);
  assert.doesNotMatch(advisoryBusyBranch[1], /UPDATE public\.banking_pay_workbench_jobs/i);
  assert.match(claim, /recovery_scan_deferral_count/i);
  assert.match(claim, /pg_try_advisory_xact_lock[\s\S]*FOR UPDATE OF claimed_job SKIP LOCKED/i);
  assert.match(claim, /UPDATE public\.banking_pay_workbench_jobs claimed_job[\s\S]*SET status='RUNNING'[\s\S]*economic_build_id=v_build_id/i);
  assert.doesNotMatch(claim, /public\.pay_workbench_claim_due_jobs\(/i);
  assert.doesNotMatch(claim, /timesheets_financials|pay_finance_case_components|pay_batch_items|pay_sync_overpayments/i);
});

test('initial source-build jobs are the only null-build jobs and RPC 1 assigns build identity before return', () => {
  assert.match(
    attempts,
    /economic_build_id IS NULL[\s\S]*job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'[\s\S]*status = 'QUEUED'[\s\S]*private_stage = 'BUILD_INITIALISE'[\s\S]*attempt_count = 0/i,
  );
  const buildInsert = claim.indexOf('INSERT INTO private.banking_pay_workbench_economic_builds');
  const attemptInsert = claim.indexOf('INSERT INTO private.banking_pay_workbench_stage_attempts');
  const result = claim.indexOf("'claimed',true");
  assert.ok(buildInsert >= 0 && attemptInsert > buildInsert && result > attemptInsert);
});

test('RPC 2 executes only the exact current nonce and fails closed without mutating stale delivery', () => {
  assert.match(execute, /CREATE OR REPLACE FUNCTION public\.pay_workbench_source_build_attempt_execute_v1\(/i);
  assert.match(execute, /attempt_row\.id=p_attempt_id[\s\S]*v_attempt\.attempt_nonce IS DISTINCT FROM p_attempt_nonce/i);
  assert.match(execute, /attempt_status <> 'STARTED'/i);
  assert.match(execute, /v_attempt\.worker_id <> v_worker_id/i);
  assert.match(execute, /v_attempt\.lane_identity <> v_lane_identity/i);
  assert.match(execute, /ATTEMPT_NOT_FOUND/i);
  assert.match(execute, /ATTEMPT_STALE_OR_SUPERSEDED/i);
  assert.match(execute, /ATTEMPT_EXPIRED/i);
  assert.match(execute, /'processed',false/i);
});

test('RPC 2 uses fixed lock order and a final 500ms lease/generation fence', () => {
  const candidateLock = execute.indexOf('FROM public.candidates AS candidate_row');
  const registryLock = execute.indexOf('FROM private.banking_pay_workbench_candidate_scope_registry AS registry');
  const buildLock = execute.indexOf('FROM private.banking_pay_workbench_economic_builds AS build_row');
  const jobLock = execute.indexOf('FROM public.banking_pay_workbench_jobs AS job_row');
  const attemptLock = execute.indexOf('FROM private.banking_pay_workbench_stage_attempts AS attempt_row', registryLock);
  assert.ok(candidateLock >= 0 && registryLock > candidateLock && buildLock > registryLock && jobLock > buildLock && attemptLock > jobLock);
  assert.match(execute, /clock_timestamp\(\) >= v_attempt\.lease_expires_at_utc-interval '500 milliseconds'/i);
  assert.match(execute, /final_attempt\.attempt_nonce=p_attempt_nonce/i);
  assert.match(execute, /final_registry\.dirty_generation=final_attempt\.captured_candidate_generation/i);
  assert.match(execute, /PAY_WORKBENCH_ATTEMPT_FINAL_FENCE_FAILED/i);
});

test('caught stage failure rolls back the inner work and leaves durable failure handling to the outer transaction', () => {
  assert.match(execute, /EXCEPTION WHEN OTHERS THEN/i);
  assert.match(execute, /inner stage subtransaction has rolled back/i);
  assert.match(execute, /public\.pay_workbench_fail_job\(/i);
  const exceptionStart = execute.indexOf('EXCEPTION WHEN OTHERS THEN');
  const finalFence = execute.indexOf('Final lease/generation/nonce fence');
  assert.ok(exceptionStart >= 0 && finalFence > exceptionStart);
  assert.doesNotMatch(execute.slice(exceptionStart, finalFence), /UPDATE private\.banking_pay_workbench_stage_attempts[\s\S]*attempt_status='FAILED'/i);
});

test('material completion and retry paths are fenced by immutable attempt evidence', () => {
  assert.match(complete, /WORKBENCH_CANDIDATE_SOURCE_BUILD/i);
  assert.match(complete, /banking_pay_workbench_stage_attempts/i);
  assert.match(complete, /attempt_status='COMPLETED'/i);
  assert.match(complete, /continuation_enqueued/i);

  assert.match(fail, /WORKBENCH_CANDIDATE_SOURCE_BUILD/i);
  assert.match(fail, /attempt_status=CASE WHEN v_is_obsolete THEN 'OBSOLETE' ELSE 'FAILED' END/i);
  assert.match(fail, /BLOCKED_UNVALIDATED_RECONCILIATION_SCALE/i);
  assert.match(fail, /v_attempt_count,0\)<COALESCE\(v_max_attempts,8\)/i);
});

test('expired delivered attempts wait for cancellation grace and receive a new nonce on retry', () => {
  assert.match(recovery, /attempt_status='STARTED'/i);
  assert.match(recovery, /lease_expires_at_utc\+interval '15 seconds'/i);
  assert.match(recovery, /attempt_status='EXPIRED'/i);
  assert.match(recovery, /status='QUEUED'/i);
  assert.match(recovery, /power\(2,LEAST\(v_expired_attempt\.attempt_count,8\)\)/i);
  assert.doesNotMatch(recovery, /SET\s+attempt_nonce/i);
  assert.match(claim, /gen_random_uuid\(\)/i);
});

test('legacy combined drain cannot execute material source-build work', () => {
  assert.doesNotMatch(
    oldDrain,
    /v_job\.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'[\s\S]{0,1200}pay_workbench_candidate_source_build_chunk\s*\(/i,
  );
  assert.match(oldDrain, /PAY_WORKBENCH_MATERIAL_SOURCE_REQUIRES_TWO_CALL_PROTOCOL/i);
});

test('attempt nonce never appears in diagnostic error payload construction', () => {
  const diagnosticPayloads = [
    ...execute.matchAll(/jsonb_build_object\(([\s\S]*?)\)/gi),
  ].map(match => match[1]);
  for (const payload of diagnosticPayloads) {
    if (/error|result_code|stage_status/i.test(payload)) {
      assert.doesNotMatch(payload, /'attempt_nonce'/i);
    }
  }
});
