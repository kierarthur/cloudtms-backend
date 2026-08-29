import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = relativePath => readFileSync(new URL(relativePath, repoRoot), 'utf8');

const replay = read('supabase/repeatable/29082026_0527_banking_pay_replaced_queue_terminal_shape_v1.sql');
const failJob = read('supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql');
const migration = read('supabase/migrations/05082026_2304_banking_pay_queue_control_acl_hardening.sql');
const canonicalBundle = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
const sourceBuildExecute = read('supabase/repeatable/04082026_1143_pay_workbench_source_build_attempt_execute_v1.sql');
const workerDrain = read('supabase/repeatable/04082026_1219_pay_workbench_worker_drain_chunk.sql');

function extractFunction(sql, name) {
  const marker = `CREATE OR REPLACE FUNCTION public.${name}(`;
  const start = sql.indexOf(marker);
  const end = sql.indexOf('\nCREATE OR REPLACE FUNCTION ', start + marker.length);
  assert.ok(start >= 0, `${name} must exist`);
  return sql.slice(start, end > start ? end : undefined);
}

test('replacement-session queue replay is service-role-only with an empty search path', () => {
  assert.match(replay, /SECURITY DEFINER\s+SET search_path = ''/i);
  assert.match(
    replay,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_session_replay_replaced_queue_v1\(uuid, uuid, text, jsonb\) FROM PUBLIC, anon, authenticated, service_role;/i,
  );
  assert.match(
    replay,
    /GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_replay_replaced_queue_v1\(uuid, uuid, text, jsonb\) TO postgres, service_role;/i,
  );
  assert.doesNotMatch(replay, /GRANT EXECUTE[^;]+\bTO\s+(?:PUBLIC|anon|authenticated)\b/i);
});

test('replacement-session replay terminalises an initial source-build job in the valid terminal identity shape', () => {
  assert.match(
    replay,
    /SET status = 'DEAD',[\s\S]*private_stage = CASE[\s\S]*job_type[\s\S]*WORKBENCH_CANDIDATE_SOURCE_BUILD[\s\S]*economic_build_id IS NULL[\s\S]*private_stage = 'BUILD_INITIALISE'[\s\S]*THEN NULL::text[\s\S]*private_cursor_kind = CASE[\s\S]*THEN NULL::text[\s\S]*private_cursor_json = CASE[\s\S]*THEN '\{\}'::jsonb[\s\S]*private_stage_version = CASE[\s\S]*THEN NULL::integer/i,
  );
  assert.match(replay, /WHERE old_job\.id = replay_job\.source_job_id[\s\S]*AND old_job\.status = 'QUEUED'/i);
});

test('legacy job failure is service-role-only with an empty search path', () => {
  assert.match(failJob, /SECURITY DEFINER\s+SET search_path = ''/i);
  assert.match(
    failJob,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_fail_job\([^;]+\) FROM PUBLIC, anon, authenticated, service_role;/i,
  );
  assert.match(failJob, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_fail_job\([^;]+\) TO postgres;/i);
  assert.match(failJob, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_fail_job\([^;]+\) TO service_role;/i);
  assert.doesNotMatch(failJob, /GRANT EXECUTE[^;]+\bTO\s+(?:PUBLIC|anon|authenticated)\b/i);
});

test('one-time migration installs the same two ACL and search-path boundaries', () => {
  assert.equal((migration.match(/SET search_path TO '';/g) || []).length, 2);
  assert.equal((migration.match(/OWNER TO postgres;/g) || []).length, 2);
  assert.match(
    migration,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_session_replay_replaced_queue_v1\(uuid, uuid, text, jsonb\)[\s\S]*FROM PUBLIC, anon, authenticated, service_role;[\s\S]*GRANT EXECUTE[\s\S]*TO postgres, service_role;/i,
  );
  assert.match(
    migration,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_fail_job\(uuid, jsonb, integer\)[\s\S]*FROM PUBLIC, anon, authenticated, service_role;[\s\S]*GRANT EXECUTE[\s\S]*TO postgres, service_role;/i,
  );
});

test('authorised replacement-session wrappers retain postgres-owned definer execution', () => {
  for (const name of [
    'pay_workbench_session_open_shared_v2',
    'pay_workbench_session_replace_after_mutation',
  ]) {
    const fn = extractFunction(canonicalBundle, name);
    assert.match(fn, /SECURITY DEFINER/i);
    assert.match(fn, /pay_workbench_session_replay_replaced_queue_v1\(/i);
  }
});

test('service-owned failure paths remain connected after direct authenticated execution is removed', () => {
  assert.match(sourceBuildExecute, /SECURITY DEFINER[\s\S]*public\.pay_workbench_fail_job\(/i);
  assert.match(sourceBuildExecute, /GRANT EXECUTE[\s\S]*TO service_role/i);
  assert.match(workerDrain, /public\.pay_workbench_fail_job\(/i);

  const cloneEligible = extractFunction(canonicalBundle, 'pay_workbench_session_clone_eligible_rows_v1');
  assert.match(cloneEligible, /SECURITY DEFINER/i);
  assert.match(cloneEligible, /public\.pay_workbench_fail_job\(/i);
});

test('empty-search-path functions schema-qualify every application relation and helper', () => {
  const applicationObjects = [
    'banking_pay_workbench_jobs',
    'banking_pay_workbench_sessions',
    'banking_pay_workbench_session_scope',
    'banking_pay_workbench_stage_attempts',
    'banking_pay_workbench_economic_builds',
    'banking_pay_workbench_candidate_source_lines',
    'banking_pay_workbench_candidate_line_work',
    'app_change_counters',
    '_audit_insert',
  ];

  for (const sql of [replay, failJob]) {
    for (const objectName of applicationObjects) {
      const unqualified = new RegExp(`(?:FROM|JOIN|UPDATE|INSERT\\s+INTO|DELETE\\s+FROM|PERFORM)\\s+${objectName}\\b`, 'i');
      assert.doesNotMatch(sql, unqualified, `${objectName} must be schema-qualified`);
    }
  }
});
