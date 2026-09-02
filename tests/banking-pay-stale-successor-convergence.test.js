import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const claimStart = readFileSync(
  new URL('../supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql', import.meta.url),
  'utf8'
).replace(/\r\n/g, '\n');
const ownerRepair = readFileSync(
  new URL('../supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql', import.meta.url),
  'utf8'
).replace(/\r\n/g, '\n');

test('expired exhausted delivery recognises an already-rebound current successor before orphan repair', () => {
  const recoveryStart = claimStart.indexOf('DELIVERED_ATTEMPT_EXHAUSTED');
  const proofStart = claimStart.indexOf('v_terminal_existing_successor_proven:=false', recoveryStart);
  const repairStart = claimStart.indexOf('public.pay_workbench_repair_orphaned_pending_source_build(', proofStart);
  const normalClaimStart = claimStart.indexOf("v_source_build_run_id := (v_job.payload_json->>'source_build_run_id')::uuid");

  assert.ok(recoveryStart >= 0, 'exhausted-delivery recovery branch is missing');
  assert.ok(proofStart > recoveryStart, 'existing-successor proof is not inside exhausted recovery');
  assert.ok(repairStart > proofStart, 'orphan repair must remain the fallback after direct proof');
  assert.ok(normalClaimStart > repairStart, 'recovery must complete before the normal claim phase');

  const proof = claimStart.slice(proofStart, repairStart);
  assert.match(proof, /scope_row\.pending_job_id IS DISTINCT FROM v_recovery\.job_id/);
  assert.match(proof, /SOURCE_BUILD_PENDING/);
  assert.match(proof, /successor\.status,''\)\)\) IN \('QUEUED','RUNNING'\)/);
  assert.match(proof, /successor\.payload_json->>'session_version'/);
  assert.match(proof, /successor_session\.version/);
  assert.match(proof, /successor\.payload_json->>'source_change_seq'/);
  assert.match(proof, /v_terminal_live_change_seq/);
  assert.match(proof, /successor\.payload_json->>'source_build_run_id'/);
  assert.match(proof, /pay_workbench_session_recompute_progress_counters/);
  assert.match(proof, /EXHAUSTED_ATTEMPT_EXISTING_SUCCESSOR_PROVEN/);
  assert.match(proof, /already_rebound_successor_proven/);
});

test('existing-successor shortcut preserves fail-closed postconditions and fallback repair', () => {
  assert.match(claimStart, /IF v_terminal_existing_successor_proven THEN[\s\S]*ELSE[\s\S]*pay_workbench_repair_orphaned_pending_source_build/);
  assert.match(claimStart, /ELSIF v_terminal_action='REBOUND_ACTIVE_SUCCESSOR'/);
  assert.match(claimStart, /v_terminal_scope\.pending_job_id IS DISTINCT FROM v_terminal_successor_job_id/);
  assert.match(claimStart, /v_terminal_owner_valid IS NOT TRUE/);
  assert.match(claimStart, /SESSION_PROGRESS_POSTCONDITION/);
  assert.match(claimStart, /PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN/);
});

test('exhausted attempt convergence uses the candidate-state terminal vocabulary accepted by the schema', () => {
  const failedCloseStart = ownerRepair.indexOf("v_candidate_action := 'FAILED_CLOSED_MAX_ATTEMPTS'");
  assert.ok(failedCloseStart >= 0, 'maximum-attempt fail-close branch is missing');

  const failedClosePrefix = ownerRepair.slice(Math.max(0, failedCloseStart - 4500), failedCloseStart + 1200);
  assert.match(failedClosePrefix, /banking_pay_workbench_session_candidate_state AS failed_state/);
  assert.match(failedClosePrefix, /SET status = 'FAILED'/);
  assert.doesNotMatch(failedClosePrefix, /SET status = 'ERROR'/);
  assert.match(failedClosePrefix, /last_error_json = jsonb_build_object/);
  assert.match(failedClosePrefix, /WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB/);

  const claimFailedCloseStart = claimStart.indexOf('IF v_terminal_action IN (');
  const claimFailedCloseEnd = claimStart.indexOf("ELSIF v_terminal_action='REBOUND_ACTIVE_SUCCESSOR'", claimFailedCloseStart);
  const claimFailedClose = claimStart.slice(claimFailedCloseStart, claimFailedCloseEnd);
  assert.match(claimFailedClose, /FAILED_CLOSED_DETERMINISTIC_SOURCE/);
  assert.match(claimFailedClose, /FAILED_CLOSED_MAX_ATTEMPTS/);
  assert.match(claimFailedClose, /v_terminal_candidate_state\.status,''\)\)\)<>'FAILED'/);
  assert.doesNotMatch(claimFailedClose, /v_terminal_candidate_state\.status,''\)\)\)<>'ERROR'/);

  const reconciledPostcondition = claimStart.slice(
    claimStart.indexOf("ELSE\n            IF UPPER", claimFailedCloseEnd),
    claimStart.indexOf("SESSION_PROGRESS_POSTCONDITION", claimFailedCloseEnd)
  );
  assert.match(reconciledPostcondition, /v_terminal_candidate_state\.status,''\)\)\)='FAILED'/);
});
