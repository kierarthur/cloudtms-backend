import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const read = (relativePath) => readFileSync(
  fileURLToPath(new URL(`../${relativePath}`, import.meta.url)),
  'utf8'
);

const repair = read('supabase/repeatable/29082026_0719_banking_pay_discarded_session_blocker_repair_v1.sql');
const worker = read('broker/src/index.js');
const verifier = read('supabase/verification/29082026_0720_banking_pay_discarded_session_blocker_verification.sql');

test('discarded-session repair is bounded to non-open source-build blockers', () => {
  assert.match(repair, /CREATE OR REPLACE FUNCTION public\.pay_workbench_repair_discarded_session_blockers_v1/i);
  assert.match(repair, /current_scope\.status[\s\S]*SOURCE_BUILD_PENDING/i);
  assert.match(repair, /stale_job\.session_id IS DISTINCT FROM current_scope\.session_id/i);
  assert.match(repair, /stale_session\.status[\s\S]*DISCARDED/i);
  assert.match(repair, /stale_session\.discarded_at_utc IS NOT NULL/i);
  assert.match(repair, /stale_job\.job_type[\s\S]*WORKBENCH_CANDIDATE_SOURCE_BUILD/i);
  assert.match(repair, /stale_job\.status[\s\S]*IN \('QUEUED','RUNNING'\)/i);
  assert.match(repair, /WHERE stale_session\.id=stale_job\.session_id[\s\S]*stale_job\.session_id IS DISTINCT FROM v_candidate\.session_id/i);
});

test('repair terminalises attempts, jobs and builds without changing current owner economics', () => {
  assert.match(repair, /attempt_status='OBSOLETE'/i);
  assert.match(repair, /SET status='DEAD'/i);
  assert.match(repair, /SET status='OBSOLETE'/i);
  assert.match(repair, /status NOT IN \('COMPLETE','OBSOLETE','FAILED'\)/i);
  assert.match(repair, /pay_workbench_repair_orphaned_pending_source_build/i);
  assert.match(repair, /pay_workbench_session_recompute_progress_counters/i);
  assert.match(repair, /PRE_DRAFT_WORKBENCH_REPAIR_ONLY/i);
  assert.doesNotMatch(repair, /(?:UPDATE|INSERT INTO|DELETE FROM)\s+(?:public\.)?(?:payment_batches|bank_payments|provider_submissions|settlements|remittances)/i);
  assert.doesNotMatch(repair, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('repair remains service-only and refreshes the PostgREST schema cache', () => {
  assert.match(repair, /SECURITY DEFINER[\s\S]*SET search_path = ''/i);
  assert.match(repair, /REVOKE ALL ON FUNCTION[\s\S]*FROM PUBLIC,anon,authenticated,service_role/i);
  assert.match(repair, /GRANT EXECUTE ON FUNCTION[\s\S]*TO postgres,service_role/i);
  assert.match(repair, /NOTIFY pgrst, 'reload schema'/i);
});

test('drain repairs discarded blockers before replay-owner repair and then uses existing claims', () => {
  const discardedAt = worker.indexOf("'pay_workbench_repair_discarded_session_blockers_v1'");
  const replayAt = worker.indexOf("'pay_workbench_repair_replayed_candidate_jobs_v1'");
  assert.ok(discardedAt > 0);
  assert.ok(replayAt > discardedAt);
  assert.match(worker.slice(discardedAt - 500, replayAt), /WORKBENCH_DISCARDED_SESSION_BLOCKER_REPAIR_FAILED/);
  assert.match(worker.slice(discardedAt - 500, replayAt), /timeoutMs: Math\.min\(8000, dbRpcHardCapMs\)/);
});

test('rollback verifier reproduces the real active blocker and proves negative boundaries', () => {
  assert.match(verifier, /_pay_workbench_candidate_serial_active_state/i);
  assert.match(verifier, /BANKING_PAY_DISCARDED_BLOCKER_FIXTURE_NOT_REPRODUCED/i);
  assert.match(verifier, /attempt_status IS DISTINCT FROM 'OBSOLETE'/i);
  assert.match(verifier, /build_status IS DISTINCT FROM 'OBSOLETE'/i);
  assert.match(verifier, /BANKING_PAY_CURRENT_OWNER_CHANGED/i);
  assert.match(verifier, /BANKING_PAY_OPEN_SESSION_WORK_WAS_TOUCHED/i);
  assert.match(verifier, /BANKING_PAY_DISCARDED_BLOCKER_REPAIR_NOT_IDEMPOTENT/i);
  assert.match(verifier, /ROLLBACK;/i);
});
