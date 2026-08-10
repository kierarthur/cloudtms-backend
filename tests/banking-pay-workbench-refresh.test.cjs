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
  path.resolve(__dirname, './fixtures/banking-pay-installed-baselines/21072026_1235_39_pay_workbench_candidate_source_build_chunk.sql'),
  'utf8'
);
const sourceBuildRuntimeFloorMigrationSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/migrations/29072026_0645_align_banking_source_build_runtime_floor.sql'),
  'utf8'
);
const scopeDiscoveryWatermarkMigrationSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/migrations/29072026_1209_banking_pay_scope_discovery_watermark.sql'),
  'utf8'
);
const boundedScopeDispatcherSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql'),
  'utf8'
);
const boundedScopeClaimSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/04082026_1141_pay_workbench_source_build_attempt_claim_start_v1.sql'),
  'utf8'
);
const boundedScopeDrainSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/04082026_1219_pay_workbench_worker_drain_chunk.sql'),
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
  assert.match(body, /scheduleBankingPayWorkbenchDrainWithDurableWake/);
  assert.match(
    body,
    /if \(candidateCount > 0 && typeof scheduleBankingPayWorkbenchDrainWithDurableWake === 'function'\)/,
    'a refresh must wake already-queued candidate work even when the idempotent enqueue inserts no new job'
  );
  assert.doesNotMatch(
    body,
    /if \(enqueuedCandidateCount > 0 && typeof scheduleBankingPayWorkbenchDrainWithDurableWake === 'function'\)/,
    'new-row count must not be the nudge gate because an interrupted continuation can already be queued'
  );
  assert.match(body, /policy_x_scope: 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'/);
  assert.match(body, /durable_wake_accepted: refreshNudge\?\.durable_wake_enqueued === true/);
  assert.match(body, /decisions_cleared: false/);
  assert.match(body, /payment_execution_started: false/);
  assert.doesNotMatch(body, /pay_workbench_session_clear_all_decisions|pay_workbench_session_discard|pay_batch_execute|pay_batch_settle/);
});

test('opening an attached shared session is read-only for continuous candidate discovery', () => {
  const body = functionBody(
    'handleBankingPayWorkbenchSessionOpen',
    'handleBankingPayWorkbenchSessionGet'
  );

  assert.match(body, /openAction === 'WORKBENCH_SESSION_ATTACHED'/);
  assert.match(body, /scope_discovery_completed: true/);
  assert.match(body, /scope_discovery_queued: false/);
  assert.match(body, /continuous_scope_maintenance: true/);
  assert.match(body, /Modal open is now read-only[\s\S]*returns materialised rows directly/);
  assert.doesNotMatch(body, /refresh_scope_kind: 'SESSION_SCOPE_DISCOVERY'/);
  assert.doesNotMatch(body, /WORKBENCH_SESSION_OPEN_SCOPE_DISCOVERY/);
  assert.doesNotMatch(body, /pay_workbench_enqueue_session_candidate_refresh/);
  assert.match(body, /inline_drain_attempted: false/);
  assert.match(body, /code: 'INLINE_DRAIN_DISABLED_ASYNC_ONLY'/);
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
  assert.match(sessionBody, /SESSION_SCOPE_DISCOVERY/);
  assert.match(sessionBody, /SESSION_FULL_LIVE/);
  assert.match(sessionBody, /pay_workbench_session_seed_scope_chunk/);
  assert.match(
    sessionBody,
    /IF UPPER\(BTRIM\(COALESCE\([\s\S]*= 'SESSION_SCOPE_DISCOVERY' THEN[\s\S]*INSERT INTO public\.banking_pay_workbench_jobs AS discovery_job[\s\S]*'WORKBENCH_SESSION_SCOPE_SEED'[\s\S]*'scope_discovery_only', true[\s\S]*RETURN jsonb_build_object\(/,
    'opening an attached session must queue bounded discovery through the existing scope-seed worker'
  );
  assert.match(
    sessionBody,
    /SELECT active_discovery_job\.id[\s\S]*active_discovery_job\.status IN \('QUEUED', 'RUNNING'\)[\s\S]*IF v_scope_discovery_job_id IS NULL THEN[\s\S]*ON CONFLICT \(dedupe_key\) WHERE status IN \('QUEUED', 'RUNNING'\)[\s\S]*DO NOTHING/,
    'repeated opens must reuse one active discovery job without updating its possibly locked row'
  );
  assert.doesNotMatch(
    sessionBody,
    /ON CONFLICT \(dedupe_key\) WHERE status IN \('QUEUED', 'RUNNING'\)[\s\S]*DO UPDATE[\s\S]*run_at_utc = LEAST/,
    'modal reopen must not wait on a queued or running discovery row merely to move its retry time'
  );
  assert.match(
    sessionBody,
    /THEN LEAST\(GREATEST\(\(v_payload_json->>'limit'\)::integer, 1\), 5\)[\s\S]*ELSE 5/,
    'each queued discovery page must remain small enough for live per-candidate eligibility checks'
  );

  const scopeSeedBody = sqlFunctionBody(
    repeatableSql,
    'pay_workbench_session_seed_scope_chunk'
  );
  assert.match(scopeSeedBody, /v_force_reseed boolean := false/);
  assert.match(scopeSeedBody, /'Rediscovering current payment candidates\.'/);
  assert.match(scopeSeedBody, /MAX\(existing_scope\.scope_ordinal\)/);
  assert.match(scopeSeedBody, /COUNT\(\*\)::integer[\s\S]*FROM public\.banking_pay_workbench_session_scope AS current_scope/);
  assert.match(scopeSeedBody, /'candidate_ids', COALESCE\(v_page_candidate_ids, '\[\]'::jsonb\)/);
  assert.match(scopeSeedBody, /WHEN v_scope_discovery_only[\s\S]*THEN COALESCE\(v_new_scope_candidate_ids, '\[\]'::jsonb\)/);
  assert.match(
    scopeSeedBody,
    /FROM public\.app_change_counters AS change_counter[\s\S]*change_counter\.updated_at > v_discovery_from_utc[\s\S]*change_counter\.updated_at <= v_discovery_to_utc/,
    'attached-session discovery must shortlist only candidates changed since the last completed discovery'
  );
  assert.match(
    scopeSeedBody,
    /NOT EXISTS \([\s\S]*FROM public\.banking_pay_workbench_session_scope AS existing_discovery_scope/,
    'existing session candidates must not be live-revalidated or rebuilt by open-time discovery'
  );
  assert.match(
    scopeSeedBody,
    /p_candidate_id => v_discovery_candidate_id[\s\S]*'scope_limit', 1/,
    'each small shortlisted candidate set must be validated through the existing live eligibility authority'
  );
  assert.match(
    scopeSeedBody,
    /IF v_scope_discovery_only THEN[\s\S]*p_candidate_id => v_discovery_candidate_id[\s\S]*ELSE[\s\S]*p_candidate_id => v_filter_candidate_id[\s\S]*'scope_limit', v_limit/,
    'the unfiltered paged scan must remain isolated to the non-discovery/manual-refresh branch'
  );
  assert.match(scopeSeedBody, /'scope_discovery_checked_at_utc', v_discovery_to_utc::text/);
  assert.match(
    scopeSeedBody,
    /v_session_row\.scope_discovery_checked_at_utc/,
    'session-open discovery must read its watermark from the durable session column'
  );
  assert.match(
    scopeSeedBody,
    /scope_discovery_checked_at_utc = CASE[\s\S]*WHEN v_scope_discovery_only AND v_scope_seed_complete[\s\S]*THEN v_discovery_to_utc/,
    'the durable discovery watermark must advance only after the terminal bounded page'
  );
  assert.doesNotMatch(
    scopeSeedBody,
    /v_session_row\.progress_json->>'scope_discovery_checked_at_utc'/,
    'later progress rendering must not be able to erase candidate-discovery authority'
  );
  assert.match(
    scopeDiscoveryWatermarkMigrationSql,
    /ALTER TABLE public\.banking_pay_workbench_sessions[\s\S]*ADD COLUMN IF NOT EXISTS scope_discovery_checked_at_utc timestamptz/,
    'the discovery watermark migration must remain idempotent'
  );
  assert.match(scopeSeedBody, /scope_discovery_changed', false/);
  assert.match(scopeSeedBody, /THEN v_session_row\.progress_state[\s\S]*ELSE 'REFRESHING_CANDIDATES'/);
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

test('job claiming acquires advisory locks only after the final bounded eligibility set and rechecks queue state atomically', () => {
  const claimBody = sqlFunctionBody(
    repeatableSql,
    'pay_workbench_claim_due_jobs'
  );

  assert.match(claimBody, /claim_source AS MATERIALIZED \(/);
  assert.doesNotMatch(
    claimBody.slice(
      claimBody.indexOf('claim_source AS MATERIALIZED ('),
      claimBody.indexOf('claim_pool AS MATERIALIZED (')
    ),
    /FOR UPDATE SKIP LOCKED/,
    'the broad candidate sample must not lock jobs that the final claim will not adopt'
  );
  assert.match(
    claimBody,
    /eligible_claim AS MATERIALIZED \([\s\S]*ORDER BY claim_pool\.priority[\s\S]*LIMIT v_limit/
  );
  assert.match(
    claimBody,
    /candidate_locked_claim AS MATERIALIZED \([\s\S]*pg_try_advisory_xact_lock\(hashtextextended\(eligible_claim\.candidate_serial_key, 24062027\)\)[\s\S]*FROM eligible_claim/
  );
  assert.match(
    claimBody,
    /hot_key_locked_claim AS MATERIALIZED \([\s\S]*FROM candidate_locked_claim[\s\S]*candidate_advisory_lock_granted IS TRUE/
  );
  assert.match(
    claimBody,
    /claim AS MATERIALIZED \([\s\S]*hot_key_advisory_lock_granted IS TRUE/
  );
  assert.doesNotMatch(
    claimBody.slice(
      claimBody.indexOf('eligible_claim AS MATERIALIZED ('),
      claimBody.indexOf('candidate_locked_claim AS MATERIALIZED (')
    ),
    /pg_try_advisory_xact_lock/,
    'advisory locks must not be acquired while PostgreSQL is still filtering the broader eligibility pool'
  );
  assert.match(
    claimBody,
    /WHERE upd_job\.id = claim_row\.id[\s\S]*upd_job\.status = 'QUEUED'[\s\S]*upd_job\.run_at_utc <= v_cutoff/,
    'the atomic UPDATE must recheck that the bounded job is still due and queued'
  );
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

test('the multi-phase Worker budget is not collapsed to one database RPC timeout', () => {
  assert.match(
    workerSource,
    /const requestedEffectiveMaxRuntimeMs = sourceBuildParallelEnabled[\s\S]*const effectiveMaxRuntimeMs = requestedEffectiveMaxRuntimeMs;/
  );
  assert.doesNotMatch(
    workerSource,
    /const effectiveMaxRuntimeMs = Math\.min\(requestedEffectiveMaxRuntimeMs, dbRpcHardCapMs\)/,
    'the outer drain must retain its configured budget while each dispatcher call remains individually capped'
  );
  assert.match(
    workerSource,
    /const passTimeoutMs = Math\.max\(1000, Math\.min\(dbRpcHardCapMs, remainingRuntimeMs\(\) - rpcSafetyBufferMs\)\)/,
    'each database call must remain bounded by the existing per-RPC hard cap'
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

test('bounded source-build runtime removes the 100-member authority and pages active facts', () => {
  const body = sqlFunctionBody(
    boundedScopeDispatcherSql,
    'pay_workbench_candidate_source_build_chunk'
  );
  assert.match(body, /v_fact_limit integer:=LEAST\(GREATEST\(COALESCE\(p_limit,25\),1\),25\)/i);
  assert.match(body, /WORKSPACE_FACT/);
  assert.match(body, /banking_pay_workbench_economic_build_fact_pages/);
  assert.doesNotMatch(body, /p_max_members|LIMIT\s+100\b/i);
});

test('database source-build delivery is split into claim/start and exact nonce execution', () => {
  assert.match(boundedScopeClaimSql, /pay_workbench_source_build_attempt_claim_start_v1/);
  assert.match(boundedScopeClaimSql, /INSERT INTO private\.banking_pay_workbench_stage_attempts/i);
  assert.match(boundedScopeClaimSql, /'attempt_nonce',v_attempt_nonce/i);
  assert.match(boundedScopeDrainSql, /PAY_WORKBENCH_MATERIAL_SOURCE_REQUIRES_TWO_CALL_PROTOCOL/);
  assert.doesNotMatch(
    boundedScopeDrainSql,
    /v_job\.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'[\s\S]{0,1200}pay_workbench_candidate_source_build_chunk\s*\(/i
  );
});
