const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const repeatableSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);
const sourceBuildSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_39_pay_workbench_candidate_source_build_chunk.sql'),
  'utf8'
);
const sourceBuildRuntimeFloorMigrationSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/migrations/29072026_0645_align_banking_source_build_runtime_floor.sql'),
  'utf8'
);

function functionBody(name, nextName) {
  const start = workerSource.indexOf(`async function ${name}`);
  assert.ok(start >= 0, `${name} must exist`);
  const end = workerSource.indexOf(`async function ${nextName}`, start + 1);
  assert.ok(end > start, `${nextName} must follow ${name}`);
  return workerSource.slice(start, end);
}

function sqlFunctionBody(source, name) {
  const functionMarker = `CREATE OR REPLACE FUNCTION public.${name}`;
  const start = source.indexOf(functionMarker);
  assert.ok(start >= 0, `${name} must exist`);
  const end = source.indexOf('CREATE OR REPLACE FUNCTION public.', start + functionMarker.length);
  return source.slice(start, end > start ? end : source.length);
}

test('workbench refresh route recomputes pre-draft live rows without payment execution', () => {
  const body = functionBody(
    'handleBankingPayWorkbenchSessionRefresh',
    'handleBankingPayWorkbenchSessionGetPreviewPage'
  );

  assert.doesNotMatch(body, /sessionRow\.actor_user_id\) !== actorUserId/, 'shared workbench refresh must not be limited to the session creator');
  assert.match(body, /sessions are shared[\s\S]*authenticated actor is still passed/i);
  assert.match(body, /sessionRow\.status\)\.toUpperCase\(\) !== 'OPEN'/);
  assert.match(body, /pay_workbench_enqueue_session_candidate_refresh/);
  assert.match(body, /refresh_scope_kind: 'SESSION_FULL_LIVE'/);
  assert.match(body, /nudgeBankingPayWorkbenchDrain/);
  assert.match(
    body,
    /if \(candidateCount > 0 && typeof nudgeBankingPayWorkbenchDrain === 'function'\)/,
    'a refresh must wake already-queued candidate work even when the idempotent enqueue inserts no new job'
  );
  assert.doesNotMatch(
    body,
    /if \(enqueuedCandidateCount > 0 && typeof nudgeBankingPayWorkbenchDrain === 'function'\)/,
    'new-row count must not be the nudge gate because an interrupted continuation can already be queued'
  );
  assert.match(body, /policy_x_scope: 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'/);
  assert.match(body, /decisions_cleared: false/);
  assert.match(body, /payment_execution_started: false/);
  assert.doesNotMatch(body, /pay_workbench_session_clear_all_decisions|pay_workbench_session_discard|pay_batch_execute|pay_batch_settle/);
});

test('refresh route is registered before generic workbench reads', () => {
  const refreshRoute = workerSource.indexOf("matchPath(p, '/api/banking/pay/workbench/session/:id/refresh')");
  const genericReadRoute = workerSource.indexOf("matchPath(p, '/api/banking/pay/workbench/session/:id')", refreshRoute);

  assert.ok(refreshRoute >= 0, 'refresh route must be registered');
  assert.ok(genericReadRoute > refreshRoute, 'refresh route must precede the generic session route');
  assert.match(
    workerSource.slice(refreshRoute, genericReadRoute),
    /req\.method === 'POST'[\s\S]*handleBankingPayWorkbenchSessionRefresh\(env, req, user, m\.id, ctx\)/
  );
});

test('explicit session refresh bypasses clone-certified reuse and forces full live source retirement', () => {
  const manyBody = sqlFunctionBody(
    repeatableSql,
    'pay_workbench_enqueue_candidate_refresh_many'
  );
  assert.match(manyBody, /v_force_refresh boolean := false/);
  assert.match(manyBody, /p_candidate_ids->>'user_requested_refresh'/);
  assert.match(manyBody, /COALESCE\(v_force_refresh, false\) IS NOT TRUE[\s\S]*v_candidate_clone_certified/);
  assert.match(manyBody, /'refresh_scope_kind', 'CANDIDATE_FULL_LIVE'/);

  const sessionStart = repeatableSql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_session_candidate_refresh');
  const sessionEnd = repeatableSql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_snapshot_refresh_candidate', sessionStart);
  assert.ok(sessionStart >= 0 && sessionEnd > sessionStart, 'session refresh function must be present');
  const sessionBody = repeatableSql.slice(sessionStart, sessionEnd);
  assert.match(sessionBody, /'force_refresh', COALESCE\(v_force_refresh, false\)/);
  assert.match(sessionBody, /'enqueued_candidate_count', COALESCE\(\(v_page_enqueue_result->>'enqueued_candidate_count'\)::integer, 0\)/);
});

test('full-live source builds retain semantic scope through internally targeted pages', () => {
  const sourceBuildBody = sqlFunctionBody(sourceBuildSql, 'pay_workbench_candidate_source_build_chunk');
  assert.match(sourceBuildBody, /'requested_refresh_scope_kind', v_requested_refresh_scope_kind/);
  assert.match(sourceBuildBody, /'actual_refresh_scope_kind', v_actual_refresh_scope_kind/);

  const lineSeedBody = sqlFunctionBody(repeatableSql, 'pay_workbench_candidate_line_work_seed');
  assert.match(lineSeedBody, /source_line\.source_row_json->>'requested_refresh_scope_kind'[\s\S]*= 'CANDIDATE_FULL_LIVE'/);
  assert.match(lineSeedBody, /COALESCE\(v_full_source_rebuild, false\)[\s\S]*existing_line_work\.timesheet_id IS NULL/);
});

test('latest-state reruns do not mutually deadlock as queued chain continuations', () => {
  const serialStateBody = sqlFunctionBody(
    repeatableSql,
    '_pay_workbench_candidate_serial_active_state'
  );

  const ownContinuationPredicate = serialStateBody.indexOf(
    "AND UPPER(BTRIM(COALESCE(v_payload->>'run_mode', ''))) NOT IN ('LATEST_STATE_HEAD', 'LATEST_RERUN_AFTER_RUNNING')"
  );
  const queuedContinuationPredicate = serialStateBody.indexOf(
    "AND UPPER(BTRIM(COALESCE(queued_job.payload_json->>'run_mode', ''))) NOT IN ('LATEST_STATE_HEAD', 'LATEST_RERUN_AFTER_RUNNING')"
  );

  assert.ok(ownContinuationPredicate >= 0, 'the current-job continuation predicate must exclude latest-state reruns');
  assert.ok(queuedContinuationPredicate > ownContinuationPredicate, 'queued jobs must use the same latest-state exclusion');
  assert.match(serialStateBody, /v_reason := 'CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION'/);
});

test('source-build lanes remain claimable inside the bounded database worker budget', () => {
  assert.match(
    sourceBuildRuntimeFloorMigrationSql,
    /banking_pay_workbench_nudge_source_build_runtime_floor_ms SET DEFAULT 8000/
  );
  assert.match(
    sourceBuildRuntimeFloorMigrationSql,
    /banking_pay_workbench_cron_source_build_runtime_floor_ms SET DEFAULT 8000/
  );
  assert.match(
    sourceBuildRuntimeFloorMigrationSql,
    /banking_pay_workbench_nudge_source_build_runtime_floor_ms = 8000/
  );
  assert.match(
    sourceBuildRuntimeFloorMigrationSql,
    /banking_pay_workbench_cron_source_build_runtime_floor_ms = 8000/
  );
  assert.doesNotMatch(
    sourceBuildRuntimeFloorMigrationSql,
    /source_build_parallelism|source_build_parallel_bursts|source_build_lane_claim_limit/,
    'the runtime-floor correction must not increase source-build concurrency or lane claim limits'
  );
});

test('source-build settled history is evaluated once per complete scope, not once per negative row', () => {
  const sourceBuildBody = sqlFunctionBody(sourceBuildSql, 'pay_workbench_candidate_source_build_chunk');

  assert.match(
    sourceBuildBody,
    /active_settled_component_basis AS \([\s\S]*_pay_active_settled_components\(\s*COALESCE\(v_sync_scope_timesheet_ids/
  );
  assert.match(
    sourceBuildBody,
    /post_active_settled_component_basis AS \([\s\S]*_pay_active_settled_components\(\s*COALESCE\(v_post_sync_scope_timesheet_ids/
  );
  assert.doesNotMatch(
    sourceBuildBody,
    /_pay_active_settled_components\(\s*ARRAY\[raw_outstanding_component\.timesheet_id\]/
  );
  assert.doesNotMatch(
    sourceBuildBody,
    /_pay_active_settled_components\(\s*ARRAY\[post_negative\.timesheet_id\]/
  );
});
