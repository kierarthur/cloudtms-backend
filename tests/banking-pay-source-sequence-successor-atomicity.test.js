import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

const enqueue = read('supabase/repeatable/07082026_1017_pay_workbench_enqueue_candidate_refresh.sql');
const repair = read('supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql');
const claim = read('supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql');
const worker = read('broker/src/index.js');

test('canonical enqueue freezes live sequence authority and synchronises it before job publication', () => {
  assert.match(enqueue, /change_counter\.scope_change_generation[\s\S]*FOR UPDATE/);
  assert.match(enqueue, /v_source_change_seq := GREATEST\([\s\S]*v_payload_source_change_seq[\s\S]*v_live_change_seq/);
  assert.match(enqueue, /scope_tx\.state[\s\S]*scope_tx\.allocated_generation[\s\S]*registry\.current_source_change_seq[\s\S]*FOR UPDATE OF scope_tx,registry/);
  assert.match(enqueue, /scope_state\.dirty_generation=v_payload_scope_change_generation/);
  assert.match(enqueue, /v_live_scope_change_generation IS DISTINCT FROM[\s\S]*v_payload_scope_change_generation/);
  assert.match(enqueue, /v_registry_dirty_generation,0\) IS DISTINCT FROM[\s\S]*v_payload_scope_change_generation/);

  const syncAt = enqueue.indexOf('UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry');
  const jobInsertAt = enqueue.indexOf('INSERT INTO public.banking_pay_workbench_jobs');
  assert.ok(syncAt >= 0 && jobInsertAt > syncAt, 'registry sequence must be accepted before the canonical job is inserted');
  assert.match(enqueue, /current_source_change_seq=GREATEST\([\s\S]*v_source_change_seq/);
  assert.match(enqueue, /PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED/);
  assert.match(enqueue, /'latest_source_change_seq',v_source_change_seq/);
});

test('orphan repair owns candidate serialisation and gives an obsolete generation a fresh successor', () => {
  const candidateLockAt = repair.indexOf('public._pay_workbench_candidate_serial_key(v_candidate.candidate_id)');
  const sessionLockAt = repair.indexOf('FROM public.banking_pay_workbench_sessions AS session_row', candidateLockAt);
  assert.ok(candidateLockAt >= 0 && sessionLockAt > candidateLockAt, 'candidate advisory ownership must precede the session row lock');
  assert.match(repair, /v_owner_generation_obsolete[\s\S]*ATTEMPT_GENERATION_OBSOLETE/);
  assert.match(repair, /v_owner_generation_obsolete IS NOT TRUE[\s\S]*v_owner\.attempt_count/);
  assert.match(repair, /v_owner_generation_obsolete[\s\S]*bounded_scope_state_precedes_job[\s\S]*scope_change_tx_token[\s\S]*scope_change_generation/);
  assert.match(repair, /v_enqueue_result := public\.pay_workbench_enqueue_candidate_refresh/);
  assert.match(repair, /PAY_WORKBENCH_OWNER_REPAIR_SUCCESSOR_INVALID/);
});

test('claim-start preserves the stale-build fence and atomically proves a current successor', () => {
  const staleFenceAt = claim.indexOf('v_build.source_change_seq IS DISTINCT FROM v_registry.current_source_change_seq');
  const obsoleteAt = claim.indexOf("SET status='OBSOLETE'", staleFenceAt);
  const repairAt = claim.indexOf('public.pay_workbench_repair_orphaned_pending_source_build', obsoleteAt);
  const returnAt = claim.indexOf("'result_code','ATTEMPT_GENERATION_OBSOLETE'", repairAt);
  assert.ok(staleFenceAt >= 0 && obsoleteAt > staleFenceAt && repairAt > obsoleteAt && returnAt > repairAt);
  assert.match(claim, /p_reason=>'ATTEMPT_GENERATION_OBSOLETE_SUCCESSOR'/);
  assert.match(claim, /SOURCE_BUILD_OBSOLETE_SUCCESSOR_NOT_PROVEN/);
  assert.match(claim, /successor_job\.status IN \('QUEUED','RUNNING'\)/);
  assert.match(claim, /successor_state\.pending_job_id=successor_job\.id/);
  assert.match(claim, /successor_state\.source_change_seq=successor_registry\.current_source_change_seq/);
  assert.match(claim, /certified_preview_publication_parity_ok/);
  assert.match(claim, /'successor_resolution'/);
});

test('the correction does not alter Worker RPC fanout, nonce handling, or financial authority', () => {
  assert.match(worker, /pay_workbench_source_build_attempt_claim_start_v1/);
  assert.match(worker, /pay_workbench_source_build_attempt_execute_v1/);
  for (const sql of [enqueue, repair, claim]) {
    assert.doesNotMatch(sql, /pay_batch_schedule|provider_submission|bank_transfer_execute|settlement_execute/i);
  }
});
