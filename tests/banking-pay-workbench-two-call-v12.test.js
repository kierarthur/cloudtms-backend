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
const sourceBuild = read('supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql');
const reconciliationEnvelope = read('supabase/migrations/06082026_0407_banking_pay_reconciliation_envelope_v2.sql');

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
  assert.match(
    claim,
    /INSERT INTO private\.banking_pay_workbench_candidate_scope_registry\(\s*candidate_id,last_dirtied_at_utc,created_at_utc,updated_at_utc\s*\)\s*VALUES\(v_job\.candidate_id,v_database_now,v_database_now,v_database_now\)/i,
  );
  assert.doesNotMatch(
    claim,
    /INSERT INTO private\.banking_pay_workbench_candidate_scope_registry\(candidate_id\)\s*VALUES\(v_job\.candidate_id\)/i,
  );
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

test('fact-page completion and creation use one database timestamp', () => {
  assert.match(sourceBuild, /v_fact_page_timestamp\s+timestamptz/i);
  assert.match(sourceBuild, /v_fact_page_timestamp:=clock_timestamp\(\);/i);
  assert.match(
    sourceBuild,
    /is_family_final,\s*completed_at_utc,created_at_utc\s*\)[\s\S]*v_is_final,\s*v_fact_page_timestamp,v_fact_page_timestamp\s*\)/i,
  );
});

test('post-seal bootstrap progress preserves the immutable terminal seed cursor', () => {
  assert.equal((sourceBuild.match(/scope_cursor_json=v_next/gi) || []).length, 3);
  assert.match(
    sourceBuild,
    /fact_cursor_json=jsonb_build_object\('cursor_kind','WORKSPACE_FACT','cursor_version',2,\s*'terminal',true,'build_id',v_build_id\),\s*updated_at_utc=clock_timestamp\(\)/i,
  );
  assert.match(sourceBuild, /bootstrap_cursor_json=v_next/i);
  assert.match(
    sourceBuild,
    /private_stage='PREPARE_SCOPE',scope_cursor_json=v_next[\s\S]*seed_scope_digest=NULL[\s\S]*seed_scope_sealed_at_utc=NULL/i,
  );
});

test('reconciliation scale authority is fail-closed and the calibration envelope is complete', () => {
  assert.match(
    sourceBuild,
    /jsonb_typeof\(v_envelope_evidence\)<>'object' OR v_envelope_evidence='\{\}'::jsonb/i,
  );
  assert.match(sourceBuild, /ENVELOPE_OR_EVIDENCE_MISSING_OR_MALFORMED/i);
  assert.match(sourceBuild, /'evidence_status','UNVALIDATED_CONFIGURATION'/i);
  assert.match(sourceBuild, /'reconciliation_scale_block',jsonb_build_object/i);

  for (const key of [
    'relevant_timesheet_count',
    'dependency_node_count',
    'dependency_edge_count',
    'settled_source_row_count',
    'settled_component_count',
    'entitlement_component_count',
    'reservation_component_count',
    'finance_case_count',
    'finance_component_count',
    'protection_evidence_count',
    'expected_case_insert_count',
    'expected_case_update_count',
    'expected_case_clear_count',
    'expected_component_insert_count',
    'expected_component_update_count',
    'expected_component_close_count',
    'canonical_source_row_count',
    'staging_bytes',
  ]) {
    assert.match(reconciliationEnvelope, new RegExp(`'${key}',\\d+`, 'i'));
  }
  assert.match(reconciliationEnvelope, /CONTROLLED_TEST_CALIBRATION/i);
  assert.match(reconciliationEnvelope, /candidate_discovery_truncated',false/i);
  assert.match(reconciliationEnvelope, /policy_x','UNCHANGED'/i);
  assert.match(reconciliationEnvelope, /BANKING_PAY_RECONCILIATION_ENVELOPE_BASELINE_CONFLICT/i);
});

test('RPC 1 attempt start, creation, update and lease use one ordered timestamp authority', () => {
  assert.match(claim, /v_attempt_started_at\s+timestamptz/i);
  assert.match(claim, /v_attempt_started_at\s*:=\s*clock_timestamp\(\);/i);
  assert.match(
    claim,
    /v_lease_expires\s*:=\s*v_attempt_started_at\+make_interval\(secs=>v_effective_lease\)/i,
  );
  assert.match(
    claim,
    /v_captured_generation,v_source_change_seq,1,v_attempt_started_at,v_lease_expires,\s*v_attempt_started_at,v_attempt_started_at/i,
  );
  assert.match(claim, /'attempt_started_at_utc',v_attempt_started_at/i);
  assert.doesNotMatch(
    claim,
    /v_captured_generation,v_source_change_seq,1,clock_timestamp\(\),v_lease_expires/i,
  );
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

test('RPC 2 final fence accepts only the exact atomically published terminal authority', () => {
  assert.match(
    execute,
    /final_registry\.current_build_id=p_build_id\s+OR\s+\([\s\S]*?v_stage_result->>'private_stage',''\)='COMPLETE'[\s\S]*?v_stage_result->>'stage_status',''\)='COMPLETE'/i,
  );
  assert.match(execute, /v_stage_result->>'has_more'\)::boolean,false\)=false/i);
  assert.match(execute, /final_registry\.current_build_id IS NULL/i);
  assert.match(execute, /final_registry\.initialisation_status='READY'/i);
  assert.match(
    execute,
    /final_registry\.evaluated_generation=final_attempt\.captured_candidate_generation/i,
  );
  assert.match(execute, /final_build\.status='COMPLETE'/i);
  assert.match(execute, /final_build\.private_stage='COMPLETE'/i);
  assert.match(execute, /final_build\.completed_at_utc IS NOT NULL/i);
  assert.match(execute, /final_build\.source_job_id=p_job_id/i);
});

test('RPC 2 absorbs transient candidate contention inside a bounded execute window', () => {
  assert.match(execute, /v_candidate_lock_wait_limit\s+interval:=interval '750 milliseconds'/i);
  assert.match(
    execute,
    /LOOP[\s\S]*pg_catalog\.pg_try_advisory_xact_lock\(v_lock_key\)[\s\S]*pg_catalog\.pg_sleep\(0\.01\)[\s\S]*END LOOP;/i,
  );
  assert.match(execute, /IF NOT v_candidate_lock_acquired THEN[\s\S]*'result_code','CANDIDATE_LOCK_BUSY'/i);
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

test('terminal material completion reaches public successful-source reconciliation', () => {
  const materialBranchStart = complete.indexOf(
    "IF v_stage_job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'",
  );
  const sourceCountParsingStart = complete.indexOf('v_source_rows_written := CASE');
  assert.ok(materialBranchStart >= 0 && sourceCountParsingStart > materialBranchStart);

  const materialBranch = complete.slice(materialBranchStart, sourceCountParsingStart);
  assert.match(
    materialBranch,
    /IF v_has_more THEN[\s\S]*UPDATE public\.banking_pay_workbench_jobs[\s\S]*RETURN jsonb_build_object[\s\S]*END IF;[\s\S]*END IF;/i,
  );
  assert.doesNotMatch(
    materialBranch,
    /END IF;\s*UPDATE public\.banking_pay_workbench_jobs[\s\S]*RETURN jsonb_build_object[\s\S]*END IF;\s*v_source_rows_written/i,
  );

  assert.match(
    complete,
    /v_source_rows_written := CASE[\s\S]*v_result_json->>'published_count'/i,
  );
  assert.match(
    complete,
    /v_current_source_row_count := CASE[\s\S]*v_result_json->>'published_count'/i,
  );
  assert.match(
    complete,
    /v_current_source_row_count_authoritative :=[\s\S]*v_stage_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'[\s\S]*v_result_json->>'private_stage'[\s\S]*v_result_json->>'stage_status'[\s\S]*v_result_json->>'published_count'/i,
  );

  const reconciliationCall = complete.indexOf(
    'public.pay_workbench_reconcile_successful_source_build(',
    sourceCountParsingStart,
  );
  const terminalJobUpdate = complete.indexOf(
    'UPDATE public.banking_pay_workbench_jobs AS update_job',
    sourceCountParsingStart,
  );
  const terminalScopeClear = complete.indexOf(
    'SET pending_job_id = NULL::uuid',
    reconciliationCall,
  );
  assert.ok(reconciliationCall > sourceCountParsingStart);
  assert.ok(terminalJobUpdate > sourceCountParsingStart && terminalJobUpdate < reconciliationCall);
  assert.ok(terminalScopeClear > reconciliationCall);
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
