const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

const repair = read('supabase/repeatable/29082026_1312_banking_pay_invalid_source_authority_repair_v1.sql');
const worker = read('broker/src/index.js');
const verifier = read('supabase/verification/29082026_1313_banking_pay_invalid_source_authority_repair_verification.sql');

test('invalid historical source authority is terminalised under the established Candidate lock', () => {
  assert.match(repair, /CREATE OR REPLACE FUNCTION public\.pay_workbench_repair_invalid_source_authority_jobs_v1\(/i);
  assert.match(repair, /pg_try_advisory_xact_lock[\s\S]*_pay_workbench_candidate_serial_key/i);
  assert.match(repair, /FOR UPDATE[\s\S]*SET status='DEAD'/i);
  assert.match(repair, /SOURCE_BUILD_AUTHORITY_PAYLOAD_INVALID_TERMINALISED/i);
  assert.match(repair, /authority_fingerprint_version[\s\S]*authority_fingerprint/i);
  assert.match(repair, /required_physical_publication_contract_version/i);
  assert.match(repair, /economic_build_id IS NULL/i);
  assert.match(repair, /invalid_job\.status='QUEUED'/i);
});

test('repair delegates replacement to canonical owner authority and proves the resulting pointer', () => {
  assert.match(repair, /public\.pay_workbench_repair_orphaned_pending_source_build\(/i);
  assert.match(repair, /created_by_helper[\s\S]*pay_workbench_enqueue_candidate_refresh/i);
  assert.match(repair, /v_state\.pending_job_id=v_owner\.id/i);
  assert.match(repair, /remaining_invalid[\s\S]*status IN \('QUEUED','RUNNING'\)/i);
  assert.match(repair, /PAY_WORKBENCH_INVALID_SOURCE_AUTHORITY_REPAIR_POSTCONDITION_FAILED/i);
  assert.match(repair, /all_state_transitions_proven/i);
  assert.match(repair, /INVALID_SOURCE_AUTHORITY_OWNER_REPAIRED/i);
});

test('repair is service-only, bounded and outside every financial lifecycle authority', () => {
  assert.match(repair, /LIMIT v_limit/i);
  assert.match(repair, /SECURITY DEFINER[\s\S]*SET search_path = ''/i);
  assert.match(repair, /REVOKE ALL ON FUNCTION[\s\S]*FROM PUBLIC,anon,authenticated,service_role/i);
  assert.match(repair, /GRANT EXECUTE ON FUNCTION[\s\S]*TO postgres,service_role/i);
  assert.match(repair, /NOTIFY pgrst, 'reload schema'/i);
  assert.doesNotMatch(repair, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(repair, /(?:UPDATE|INSERT INTO|DELETE FROM)\s+(?:public\.)?(?:pay_batches|bank_payments|provider_submissions|settlements|remittances)/i);
});

test('worker settles invalid source authority before any two-call source claim and fails closed', () => {
  const drainStart = worker.indexOf("logDrainDiag('WORKBENCH_DRAIN_START'");
  const replayRepair = worker.indexOf("'pay_workbench_repair_replayed_candidate_jobs_v1'", drainStart);
  const authorityRepair = worker.indexOf("'pay_workbench_repair_invalid_source_authority_jobs_v1'", drainStart);
  const firstSourceClaim = worker.indexOf('pay_workbench_source_build_attempt_claim_start_v1', drainStart);

  assert.ok(drainStart >= 0);
  assert.ok(replayRepair > drainStart);
  assert.ok(authorityRepair > replayRepair);
  assert.ok(firstSourceClaim > authorityRepair);
  assert.match(worker.slice(authorityRepair, firstSourceClaim), /remaining_invalid_active_count/i);
  assert.match(worker.slice(authorityRepair, firstSourceClaim), /all_state_transitions_proven/i);
  assert.match(worker.slice(authorityRepair, firstSourceClaim), /WORKBENCH_INVALID_SOURCE_AUTHORITY_REPAIR_FAILED[\s\S]*throw repairError/i);
});

test('repair result is retained in bounded drain evidence without exposing payload details', () => {
  assert.match(worker, /invalid_source_authority_owner_repair:\s*invalidSourceAuthorityRepair/i);
  assert.match(worker, /WORKBENCH_INVALID_SOURCE_AUTHORITY_REPAIRED/i);
  assert.doesNotMatch(
    worker.slice(worker.indexOf("logDrainDiag('WORKBENCH_INVALID_SOURCE_AUTHORITY_REPAIRED'"),
      worker.indexOf("logDrainDiag('WORKBENCH_INVALID_SOURCE_AUTHORITY_REPAIRED'") + 700),
    /candidate_id|session_id|authority_fingerprint\s*:/i
  );
});

test('rollback verifier reproduces the exact claim failure and proves terminal, owner and financial boundaries', () => {
  assert.match(verifier, /^BEGIN;/m);
  assert.match(verifier, /ROLLBACK;/m);
  assert.match(verifier, /PAY_WORKBENCH_SOURCE_BUILD_AUTHORITY_FINGERPRINT_REQUIRED/i);
  assert.match(verifier, /BANKING_PAY_INVALID_AUTHORITY_OLD_FAILURE_NOT_REPRODUCED/i);
  assert.match(verifier, /terminalised_job_count[\s\S]*<>3/i);
  assert.match(verifier, /BANKING_PAY_INVALID_AUTHORITY_DEAD_JOB_REMAINS_CURRENT/i);
  assert.match(verifier, /BANKING_PAY_INVALID_AUTHORITY_CURRENT_OWNER_NOT_PRESERVED/i);
  assert.match(verifier, /BANKING_PAY_INVALID_AUTHORITY_REPAIR_NOT_IDEMPOTENT/i);
  assert.match(verifier, /BANKING_PAY_INVALID_AUTHORITY_FINANCIAL_LIFECYCLE_CHANGED/i);
  assert.match(verifier, /BANKING_PAY_INVALID_AUTHORITY_REPAIR_ACL_INVALID/i);
});
