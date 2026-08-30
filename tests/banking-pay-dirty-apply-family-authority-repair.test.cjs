const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const dirtyRuntime = fs.readFileSync(
  path.join(root, 'supabase/repeatable/07082026_1016_banking_pay_targeted_delta_runtime.sql'),
  'utf8',
);
const repair = fs.readFileSync(
  path.join(root, 'supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql'),
  'utf8',
);
const worker = fs.readFileSync(path.join(root, 'broker/src/index.js'), 'utf8');

test('dirty apply proves the final family scope and reissues one canonical authority before session fan-out', () => {
  const start = dirtyRuntime.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(',
  );
  assert.notEqual(start, -1);
  const body = dirtyRuntime.slice(start);

  const familyNormalisation = body.indexOf(
    'public._pay_workbench_normalise_timesheet_rotation_scope_payload',
  );
  const proof = body.indexOf('v_preceding_scope_authority_reusable :=', familyNormalisation);
  const reissue = body.indexOf('private.pay_workbench_scope_invalidate_v1(', proof);
  const sessionScan = body.indexOf('FOR v_session_row IN', reissue);
  const canonicalEnqueue = body.indexOf('public.pay_workbench_enqueue_candidate_refresh(', sessionScan);

  assert.ok(familyNormalisation > -1);
  assert.ok(proof > familyNormalisation);
  assert.ok(reissue > proof);
  assert.ok(sessionScan > reissue);
  assert.ok(canonicalEnqueue > sessionScan);
  assert.match(body.slice(familyNormalisation, reissue), /banking_pay_scope_change_transactions/i);
  assert.match(body.slice(familyNormalisation, reissue), /banking_pay_workbench_timesheet_scope_state/i);
  assert.match(body.slice(familyNormalisation, reissue), /scope_change_generation/i);
  assert.match(body.slice(reissue, sessionScan), /skip_candidate_job_enqueue['"\s,:]+true/i);
  assert.match(body.slice(reissue, sessionScan), /PAY_WORKBENCH_EFFECTIVE_SCOPE_REISSUE_NOT_STAGED/i);
  assert.match(body.slice(reissue, sessionScan), /public\._change_bump\('pay_candidate:'/i);
  assert.match(body.slice(reissue, sessionScan), /preinvalidated_scope_reissue_pending_finalization/i);
  assert.match(body.slice(reissue, sessionScan), /SET status='QUEUED'/i);
  assert.doesNotMatch(body.slice(reissue, sessionScan), /SET CONSTRAINTS/i);
  assert.match(body, /preinvalidated_scope_reissued/i);
});

test('repair targets only the two proved terminal dirty-apply failures and retains DEAD history', () => {
  assert.match(repair, /CREATE OR REPLACE FUNCTION public\.pay_workbench_repair_invalid_dirty_apply_jobs_v1\(/i);
  assert.match(repair, /PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED/i);
  assert.match(repair, /PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT/i);
  assert.match(repair, /invalid_job\.status='DEAD'/i);
  assert.match(repair, /successful_job\.status='SUCCEEDED'/i);
  assert.match(repair, /pg_try_advisory_xact_lock/i);
  assert.match(repair, /private\.pay_workbench_scope_invalidate_v1\(/i);
  assert.match(repair, /ARRAY\[NULL::uuid\]/i);
  assert.match(repair, /successor_job\.status IN \('QUEUED','RUNNING'\)/i);
  assert.match(repair, /v_successor_tx_state IS DISTINCT FROM 'PENDING'/i);
  assert.match(repair, /v_registry_tx_token IS DISTINCT FROM v_successor\.scope_change_tx_token/i);
  assert.match(repair, /successor_finalization_staged/i);
  assert.match(repair, /PAY_WORKBENCH_INVALID_DIRTY_APPLY_REPAIR_POSTCONDITION_FAILED/i);
  assert.doesNotMatch(repair, /UPDATE public\.banking_pay_workbench_jobs AS invalid_job[\s\S]*SET status/i);
  assert.match(repair, /FROM PUBLIC,anon,authenticated/i);
  assert.match(repair, /TO postgres,service_role/i);
});

test('Workbench drain runs the bounded dirty-apply repair before ordinary claims', () => {
  const drainStart = worker.indexOf('async function drainBankingPayWorkbenchJobs');
  assert.notEqual(drainStart, -1);
  const repairCall = worker.indexOf("'pay_workbench_repair_invalid_dirty_apply_jobs_v1'", drainStart);
  const firstClaim = worker.indexOf("'pay_workbench_worker_drain_chunk_revalidated_v1'", repairCall);
  assert.ok(repairCall > drainStart);
  assert.ok(firstClaim > repairCall);
  assert.match(worker.slice(repairCall, firstClaim), /all_state_transitions_proven/i);
  assert.match(worker.slice(repairCall, firstClaim), /remaining_invalid_unrepaired_count/i);
  assert.match(worker.slice(repairCall, firstClaim), /WORKBENCH_INVALID_DIRTY_APPLY_REPAIR_FAILED/i);
});
