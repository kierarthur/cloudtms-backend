const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repeatable = (name) => fs.readFileSync(
  path.resolve(__dirname, `../supabase/repeatable/${name}`),
  'utf8'
);

const extractFunction = (source, name) => {
  const startPattern = new RegExp(
    `^CREATE OR REPLACE FUNCTION public\\.${name}\\s*\\(`,
    'm'
  );
  const match = startPattern.exec(source);
  assert.ok(match, `${name} must have a canonical definition`);
  const tail = source.slice(match.index);
  const endMatch = /\r?\n\$function\$;/.exec(tail);
  assert.ok(endMatch, `${name} must have a complete function delimiter`);
  return tail.slice(0, endMatch.index + endMatch[0].length);
};

const listSqlFiles = (directory) => fs.readdirSync(directory, { withFileTypes: true })
  .flatMap((entry) => {
    const fullPath = path.join(directory, entry.name);
    return entry.isDirectory()
      ? listSqlFiles(fullPath)
      : (entry.isFile() && entry.name.endsWith('.sql') ? [fullPath] : []);
  });

const canonicalBundleSql = repeatable('26052026_2100HRS_NEW_FUNCTIONS.sql');
const repairSql = repeatable('30072026_1310_pay_workbench_repair_orphaned_pending_source_build.sql');
const claimSql = extractFunction(canonicalBundleSql, 'pay_workbench_claim_due_jobs');
const failSql = extractFunction(canonicalBundleSql, 'pay_workbench_fail_job');
const drainSql = extractFunction(canonicalBundleSql, 'pay_workbench_worker_drain_chunk');
const progressSql = extractFunction(canonicalBundleSql, 'pay_workbench_session_get_progress_light');
const recomputeSql = extractFunction(canonicalBundleSql, 'pay_workbench_session_recompute_progress_counters');
const workerSource = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');
const affectedFunctions = [
  'pay_workbench_repair_orphaned_pending_source_build',
  'pay_workbench_claim_due_jobs',
  'pay_workbench_fail_job',
  'pay_workbench_worker_drain_chunk',
  'pay_workbench_session_get_progress_light',
  'pay_workbench_session_recompute_progress_counters'
];

test('each affected function has exactly one latest definition across migrations and repeatables', () => {
  const sqlFiles = [
    ...listSqlFiles(path.resolve(__dirname, '../supabase/migrations')),
    ...listSqlFiles(path.resolve(__dirname, '../supabase/repeatable'))
  ];
  for (const functionName of affectedFunctions) {
    const pattern = new RegExp(
      `CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+public\\.${functionName}\\s*\\(`,
      'gi'
    );
    const definitions = sqlFiles.flatMap((file) => {
      const source = fs.readFileSync(file, 'utf8');
      return [...source.matchAll(pattern)].map(() => path.relative(
        path.resolve(__dirname, '..'),
        file
      ));
    });
    assert.deepEqual(
      definitions.length,
      1,
      `${functionName} must have exactly one definition, found: ${definitions.join(', ')}`
    );
  }
});

test('claiming invokes the bounded owner repair without changing canonical enqueue authority', () => {
  assert.match(claimSql, /pay_workbench_repair_orphaned_pending_source_build/);
  assert.match(claimSql, /LEAST\(GREATEST\(v_limit, 1\), 10\)/);
  assert.match(repairSql, /pay_workbench_enqueue_candidate_refresh/);
  assert.match(repairSql, /pay_workbench_reconcile_successful_source_build/);
  assert.match(repairSql, /force_legacy', true/);
  assert.match(repairSql, /policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'/);
  assert.doesNotMatch(repairSql, /pay_batch|provider|settlement|remittance/i);
});

test('owner repair validates the successor before leaving a scope pending', () => {
  assert.match(repairSql, /successor_scope\.pending_job_id = successor_job\.id/);
  assert.match(repairSql, /successor_job\.payload_json->>'session_version'/);
  assert.match(repairSql, /successor_job\.payload_json->>'source_change_seq'/);
  assert.match(repairSql, /successor_job\.payload_json->>'source_build_run_id'/);
  assert.match(repairSql, /WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB/);
  assert.match(repairSql, /FAILED_CLOSED_REPAIR_ERROR/);
});

test('terminal failure clears only the failed job ownership and preserves a newer successor', () => {
  assert.match(failSql, /pending_job_id = NULL::uuid/);
  assert.match(
    failSql,
    /pending_job_id = p_job_id[\s\S]*OR source_scope_row\.pending_job_id IS NULL/
  );
  assert.doesNotMatch(
    failSql,
    /source_scope_row\.candidate_id = v_job_candidate_id;\s*$/m,
    'source terminalisation must not update an unguarded candidate scope'
  );
  assert.doesNotMatch(failSql, /'job_error_json', COALESCE\(p_error_json/);
});

test('secondary failure-handler errors requeue while attempts remain and fail closed only at max attempts', () => {
  assert.match(drainSql, /failure_fallback_decision', v_failure_fallback_decision/);
  assert.match(drainSql, /REQUEUE_WHILE_ATTEMPTS_REMAIN/);
  assert.match(drainSql, /TERMINAL_ONLY_AT_MAX_ATTEMPTS/);
  assert.match(drainSql, /attempt_count, 0\) < COALESCE\(v_job_row\.max_attempts, 8\)/);
  assert.match(drainSql, /PAY_WORKBENCH_FAILURE_FALLBACK_DID_NOT_TRANSITION_JOB/);
  assert.match(drainSql, /failure_fallback_audit_failed/);
});

test('progress and recompute classify ownerless pending work as terminal recovery, not endless work', () => {
  for (const sql of [progressSql, recomputeSql]) {
    assert.match(sql, /WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB/);
    assert.match(sql, /recovery_required_count/);
    assert.match(sql, /recovery_scheduled_count/);
    assert.match(sql, /pending_owner_failures/);
    assert.match(sql, /RECOVERY_REQUIRED/);
    assert.match(sql, /AUTOMATIC_RECOVERY/);
  }
});

test('backend draft gate passes recovery evidence through and blocks invalid ownership', () => {
  assert.match(workerSource, /malformedFields\.push\('recovery_required'\)/);
  assert.match(workerSource, /malformedFields\.push\('recovery_scheduled'\)/);
  assert.match(workerSource, /WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB/);
  assert.match(workerSource, /pending_owner_failures: Array\.isArray/);
  assert.match(workerSource, /BANKING_PAY_WORKBENCH_RECOVERY_REQUIRED/);
});
