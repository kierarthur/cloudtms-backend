import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const canonical = read('supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql');
const lateAuthority = read('supabase/repeatable/08082026_0313_pay_workbench_fail_job_authority.sql');

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
