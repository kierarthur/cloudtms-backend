import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const canonical = read('supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql');
const lateAuthority = read('supabase/repeatable/08082026_0313_pay_workbench_fail_job_authority.sql');
const repair = read('supabase/migrations/08082026_0315_repair_workbench_requeued_source_attempts.sql');

test('late authority reasserts the focused fail-job owner after historical omnibus files', () => {
  assert.match(lateAuthority, /\\ir 04082026_1219_pay_workbench_fail_job\.sql/);
  assert.match(canonical, /FROM private\.banking_pay_workbench_stage_attempts attempt/i);
  assert.match(canonical, /attempt\.attempt_status='STARTED'/i);
  assert.match(canonical, /SET\s+attempt_status=CASE WHEN v_is_obsolete THEN 'OBSOLETE' ELSE 'FAILED' END/i);
  assert.match(canonical, /WHERE id=v_attempt_id AND attempt_status='STARTED'/i);
  assert.match(canonical, /SET search_path = ''/i);
});

test('source-build retry cannot leave a private STARTED attempt behind', () => {
  const attemptTerminal = canonical.indexOf("attempt_status=CASE WHEN v_is_obsolete THEN 'OBSOLETE' ELSE 'FAILED' END");
  const publicRequeue = canonical.indexOf("status='QUEUED',run_at_utc=v_next_run_at_utc");
  assert.ok(attemptTerminal >= 0, 'private attempt terminalisation must exist');
  assert.ok(publicRequeue > attemptTerminal, 'private attempt must be terminal before public job requeue');
  assert.match(canonical, /REVOKE ALL ON FUNCTION public\.pay_workbench_fail_job[\s\S]*FROM PUBLIC, anon, authenticated, service_role/i);
  assert.match(canonical, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_fail_job[\s\S]*TO service_role/i);
});

test('deterministic semantic publication failure terminalises once and proves the replacement owner atomically', () => {
  assert.match(canonical, /CERTIFIED_SOURCE_PREVIEW_SEMANTIC_PARITY_FAILED/);
  assert.match(canonical, /DETERMINISTIC_STAGE_ERROR/);
  assert.match(canonical, /pay_workbench_repair_orphaned_pending_source_build/);
  assert.match(canonical, /TERMINAL_SOURCE_STAGE_FAILURE_ATOMIC_REPAIR/);
  assert.match(canonical, /PAY_WORKBENCH_TERMINAL_FAILURE_SUCCESSOR_NOT_PROVEN/);
  assert.match(canonical, /terminal_owner\.status, ''\)\)\) IN \('QUEUED', 'RUNNING'\)/);
  assert.match(canonical, /terminal_candidate_state[\s\S]*pending_job_id = NULL::uuid/);
  assert.match(canonical, /terminal_successor_proven/);
  assert.ok(
    canonical.indexOf('pg_advisory_xact_lock') < canonical.indexOf('WHERE id=p_job_id FOR UPDATE'),
    'candidate serial ownership must precede the source job row lock',
  );
});

test('orphan repair is bounded to expired deterministic source-stage failures', () => {
  assert.match(repair, /v_target_count>10/i);
  assert.match(repair, /attempt\.attempt_status='STARTED'/i);
  assert.match(repair, /attempt\.lease_expires_at_utc\+interval '15 seconds'<clock_timestamp\(\)/i);
  assert.match(repair, /job\.status='QUEUED'/i);
  assert.match(repair, /job\.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'/i);
  assert.match(repair, /job\.economic_build_id=attempt\.build_id/i);
  assert.match(repair, /job\.private_stage=attempt\.private_stage/i);
  assert.match(repair, /job\.last_error_json->>'code'='23514'/i);
  assert.match(repair, /SET attempt_status='FAILED'/i);
  assert.match(repair, /error_class='DETERMINISTIC_STAGE_ERROR'/i);
  assert.doesNotMatch(repair, /UPDATE\s+public\.banking_pay_workbench_jobs/i);
  assert.doesNotMatch(repair, /UPDATE\s+private\.banking_pay_workbench_economic_builds/i);
  assert.doesNotMatch(repair, /DELETE\s+FROM/i);
});
