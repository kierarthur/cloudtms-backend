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

function functionBody(name, nextName) {
  const start = workerSource.indexOf(`async function ${name}`);
  assert.ok(start >= 0, `${name} must exist`);
  const end = workerSource.indexOf(`async function ${nextName}`, start + 1);
  assert.ok(end > start, `${nextName} must follow ${name}`);
  return workerSource.slice(start, end);
}

test('workbench refresh route recomputes pre-draft live rows without payment execution', () => {
  const body = functionBody(
    'handleBankingPayWorkbenchSessionRefresh',
    'handleBankingPayWorkbenchSessionGetPreviewPage'
  );

  assert.match(body, /sessionRow\.actor_user_id\) !== actorUserId/);
  assert.match(body, /sessionRow\.status\)\.toUpperCase\(\) !== 'OPEN'/);
  assert.match(body, /pay_workbench_enqueue_session_candidate_refresh/);
  assert.match(body, /refresh_scope_kind: 'SESSION_FULL_LIVE'/);
  assert.match(body, /nudgeBankingPayWorkbenchDrain/);
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
  const manyStart = repeatableSql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh_many');
  const manyEnd = repeatableSql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_preview_page', manyStart);
  assert.ok(manyStart >= 0 && manyEnd > manyStart, 'candidate refresh-many function must be present');
  const manyBody = repeatableSql.slice(manyStart, manyEnd);
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
