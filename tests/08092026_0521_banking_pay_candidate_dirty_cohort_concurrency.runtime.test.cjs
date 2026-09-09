const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const test = require('node:test');

const container = String(process.env.H12_DIRTY_COHORT_CONCURRENCY_CONTAINER || '');
const database = String(process.env.H12_DIRTY_COHORT_CONCURRENCY_DATABASE || '');
const enabled = /^h12-dirty-coalesce-pg(?:17|18)$/.test(container)
  && /^h12_[a-z0-9_]+_pg(?:17|18)$/.test(database);

const literal = value => `'${String(value).replaceAll("'", "''")}'`;
const args = () => [
  'exec', '-i', container, 'psql', '-X', '-qAt',
  '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
];

function run(sql, timeout = 30000) {
  const result = spawnSync('docker', args(), { input: sql, encoding: 'utf8', timeout });
  assert.equal(result.status, 0, String(result.stderr || result.error?.message || result.stdout).slice(0, 4000));
  return result.stdout;
}

function runAsync(sql) {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', args(), { stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', status => resolve({ status, stdout, stderr }));
    child.stdin.end(sql);
  });
}

function json(output) {
  const lines = String(output).trim().split(/\r?\n/).filter(Boolean);
  assert.ok(lines.length > 0, 'expected JSON output');
  return JSON.parse(lines.at(-1));
}

async function waitForLockWait(applicationName, timeoutMs = 4000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const count = Number(run(`
      SELECT count(*)
      FROM pg_catalog.pg_stat_activity
      WHERE application_name=${literal(applicationName)}
        AND wait_event_type='Lock';
    `).trim());
    if (count === 1) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert.fail(`leader ${applicationName} never reached the deterministic row-lock snapshot window`);
}

function setupFixture({ candidateId, timesheetIds, jobIds, prefix }) {
  const candidateRef = `CCR-${String(parseInt(candidateId.slice(0, 7), 16) + 100000000)}`;
  const timesheetValues = timesheetIds.map((id, index) => (
    `(${literal(id)}::uuid,${literal(`${prefix}:BOOKING:${index + 1}`)},` +
    `${literal(prefix)},'VERIFY','VERIFY','VERIFY','2099-09-06','RECEIVED',true,1)`
  )).join(',');
  const financialValues = timesheetIds.map(id => (
    `(${literal(id)}::uuid,1,true,${literal(candidateId)}::uuid,'ASSIGNED','READY_FOR_HR')`
  )).join(',');
  const jobValues = jobIds.map((id, index) => `(
    ${literal(id)}::uuid,'WORKBENCH_CANDIDATE_DIRTY_APPLY','QUEUED',-1000,
    clock_timestamp(),0,8,${literal(`${prefix}:JOB:${index + 1}`)},
    ${literal(candidateId)}::uuid,
    jsonb_build_object(
      'candidate_id',${literal(candidateId)},
      'targeted_timesheet_ids',jsonb_build_array(${literal(timesheetIds[index])}),
      'linked_timesheet_ids','[]'::jsonb,'finance_case_ids','[]'::jsonb,
      'reason_latest','DIRTY_TRIGGER:TIMESHEETS:UPDATE',
      'reasons',jsonb_build_array('DIRTY_TRIGGER:TIMESHEETS:UPDATE'),
      'latest_source_change_seq',${index + 1},
      'source_change_seq',${index + 1},
      'source_change_sequence',${index + 1},
      'latest_event_at_utc',clock_timestamp()
    ),clock_timestamp(),clock_timestamp()
  )`).join(',');
  run(`
    BEGIN;
    SET LOCAL statement_timeout='30s';
    SET LOCAL session_replication_role='replica';
    INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
    VALUES (${literal(candidateId)}::uuid,${literal(prefix)},${literal(candidateRef)},'PAYE');
    INSERT INTO public.timesheets(
      timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
      job_title_norm,week_ending_date,status,is_current,version
    ) VALUES ${timesheetValues};
    INSERT INTO public.timesheets_financials(
      timesheet_id,timesheet_version,is_current,candidate_id,
      candidate_assignment,processing_status
    ) VALUES ${financialValues};
    INSERT INTO public.app_change_counters(entity_key,seq,scope_change_generation)
    VALUES ('pay_candidate:'||${literal(candidateId)},1000,0)
    ON CONFLICT(entity_key) DO UPDATE SET seq=GREATEST(public.app_change_counters.seq,1000);
    SET LOCAL session_replication_role='origin';
    INSERT INTO public.banking_pay_workbench_jobs(
      id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,
      dedupe_key,candidate_id,payload_json,created_at_utc,updated_at_utc
    ) VALUES ${jobValues};
    COMMIT;
    BEGIN;
    UPDATE public.banking_pay_workbench_jobs
    SET status='RUNNING',attempt_count=1,started_at_utc=clock_timestamp(),
        updated_at_utc=clock_timestamp()
    WHERE id=ANY(ARRAY[${jobIds.map(id => `${literal(id)}::uuid`).join(',')}]);
    COMMIT;
  `);
}

function cleanupFixture(candidateId) {
  run(`
    BEGIN;
    SET LOCAL session_replication_role='replica';
    DELETE FROM public.banking_pay_workbench_jobs
    WHERE candidate_id=${literal(candidateId)}::uuid
       OR (candidate_id IS NULL AND lower(btrim(COALESCE(payload_json->>'candidate_id','')))
             =${literal(candidateId)});
    DELETE FROM private.banking_pay_workbench_timesheet_scope_state
    WHERE candidate_id=${literal(candidateId)}::uuid;
    DELETE FROM private.banking_pay_workbench_candidate_scope_registry
    WHERE candidate_id=${literal(candidateId)}::uuid;
    DELETE FROM public.app_change_counters
    WHERE entity_key='pay_candidate:'||${literal(candidateId)};
    DELETE FROM public.timesheets_financials
    WHERE candidate_id=${literal(candidateId)}::uuid;
    DELETE FROM public.timesheets
    WHERE booking_id LIKE ${literal(`H12-DIRTY-COHORT-CONCURRENCY:${candidateId}:%`)};
    DELETE FROM public.candidates WHERE id=${literal(candidateId)}::uuid;
    COMMIT;
  `);
}

test('two simultaneously RUNNING workers requeue the loser promptly and converge under one finalized token', {
  skip: !enabled,
  timeout: 30000,
}, async () => {
  const candidateId = randomUUID();
  const timesheetIds = [randomUUID(), randomUUID(), randomUUID()];
  const jobIds = [randomUUID(), randomUUID(), randomUUID()].sort();
  const prefix = `H12-DIRTY-COHORT-CONCURRENCY:${candidateId}`;
  setupFixture({ candidateId, timesheetIds, jobIds, prefix });
  try {
    // Keep the lowest-ID member queued, then lock it from a third session. The
    // middle-ID public processor acquires Candidate authority and blocks on the
    // deterministic first cohort row before it can lock the highest-ID member;
    // that highest-ID public processor can therefore reach the advisory gate.
    run(`
      UPDATE public.banking_pay_workbench_jobs
      SET status='QUEUED',attempt_count=0,started_at_utc=NULL,
          updated_at_utc=clock_timestamp()
      WHERE id=${literal(jobIds[0])}::uuid;
    `);
    const blocker = runAsync(`
      BEGIN;
      SET LOCAL statement_timeout='15s';
      SELECT id FROM public.banking_pay_workbench_jobs
      WHERE id=${literal(jobIds[0])}::uuid FOR UPDATE;
      SELECT pg_catalog.pg_sleep(3);
      COMMIT;
    `);
    await new Promise(resolve => setTimeout(resolve, 150));
    const winnerApplicationName = `h12-dirty-winner-${candidateId}`;
    const winner = runAsync(`
      BEGIN;
      SET LOCAL application_name=${literal(winnerApplicationName)};
      SET LOCAL statement_timeout='15s';
      SET LOCAL lock_timeout='10s';
      SELECT public.pay_workbench_candidate_dirty_apply_job_process(
        ${literal(jobIds[1])}::uuid,100
      )::text;
      COMMIT;
    `);
    await waitForLockWait(winnerApplicationName);
    const loserStarted = Date.now();
    const loser = json(run(`
      SET statement_timeout='10s';
      SET lock_timeout='5s';
      SELECT public.pay_workbench_candidate_dirty_apply_job_process(
        ${literal(jobIds[2])}::uuid,100
      )::text;
    `));
    const loserElapsedMs = Date.now() - loserStarted;
    assert.equal(loser.candidate_serial_delayed, true);
    assert.equal(loser.reason, 'CANDIDATE_SERIAL_LOCK_BUSY');
    assert.ok(loserElapsedMs < 2000, `loser took ${loserElapsedMs}ms`);

    const [winnerDone, blockerDone] = await Promise.all([winner, blocker]);
    assert.equal(winnerDone.status, 0, winnerDone.stderr);
    assert.equal(blockerDone.status, 0, blockerDone.stderr);
    const leaderResult = json(winnerDone.stdout);
    assert.equal(leaderResult.dirty_apply_cohort_action, 'COHORT_REISSUED_PENDING_FINALIZATION');
    assert.equal(leaderResult.dirty_apply_cohort_member_count, 3);

    const state = json(run(`
      SELECT jsonb_build_object(
        'queued_count',(SELECT count(*) FROM public.banking_pay_workbench_jobs
          WHERE id=ANY(ARRAY[${jobIds.map(id => `${literal(id)}::uuid`).join(',')}])
            AND status='QUEUED' AND attempt_count=0 AND started_at_utc IS NULL),
        'token_count',(SELECT count(DISTINCT payload_json->>'scope_change_tx_token')
          FROM public.banking_pay_workbench_jobs
          WHERE id=ANY(ARRAY[${jobIds.map(id => `${literal(id)}::uuid`).join(',')}])),
        'generation_count',(SELECT count(DISTINCT scope_change_generation)
          FROM public.banking_pay_workbench_jobs
          WHERE id=ANY(ARRAY[${jobIds.map(id => `${literal(id)}::uuid`).join(',')}])),
        'cohort_marker_count',(SELECT count(*) FROM public.banking_pay_workbench_jobs
          WHERE id=ANY(ARRAY[${jobIds.map(id => `${literal(id)}::uuid`).join(',')}])
            AND payload_json->>'dirty_apply_cohort_contract_version'='DIRTY_APPLY_COHORT_AUTHORITY_V1')
      )::text;
    `));
    assert.deepEqual(state, {
      queued_count: 3,
      token_count: 1,
      generation_count: 1,
      cohort_marker_count: 3,
    });
  } finally {
    cleanupFixture(candidateId);
  }
});

test('a distinct job inserted after the leader snapshot is admitted only to the next cohort', {
  skip: !enabled,
  timeout: 30000,
}, async () => {
  const candidateId = randomUUID();
  const timesheetIds = [randomUUID(), randomUUID(), randomUUID()];
  const jobIds = [randomUUID(), randomUUID()];
  const lateJobId = randomUUID();
  const prefix = `H12-DIRTY-COHORT-CONCURRENCY:${candidateId}`;
  setupFixture({ candidateId, timesheetIds: timesheetIds.slice(0, 2), jobIds, prefix });
  // Add only the third owned Timesheet; its dirty job must be inserted later.
  run(`
    BEGIN;
    SET LOCAL session_replication_role='replica';
    INSERT INTO public.timesheets(
      timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
      job_title_norm,week_ending_date,status,is_current,version
    ) VALUES (
      ${literal(timesheetIds[2])}::uuid,${literal(`${prefix}:BOOKING:3`)},
      ${literal(prefix)},'VERIFY','VERIFY','VERIFY','2099-09-06','RECEIVED',true,1
    );
    INSERT INTO public.timesheets_financials(
      timesheet_id,timesheet_version,is_current,candidate_id,
      candidate_assignment,processing_status
    ) VALUES (
      ${literal(timesheetIds[2])}::uuid,1,true,${literal(candidateId)}::uuid,
      'ASSIGNED','READY_FOR_HR'
    );
    COMMIT;
  `);
  try {
    const blocker = runAsync(`
      BEGIN;
      SET LOCAL statement_timeout='15s';
      SELECT id FROM public.banking_pay_workbench_jobs
      WHERE id=${literal(jobIds[1])}::uuid FOR UPDATE;
      SELECT pg_catalog.pg_sleep(5);
      COMMIT;
    `);
    await new Promise(resolve => setTimeout(resolve, 200));
    const applicationName = `h12-dirty-late-${candidateId}`;
    const leader = runAsync(`
      BEGIN;
      SET LOCAL application_name=${literal(applicationName)};
      SET LOCAL statement_timeout='15s';
      SET LOCAL lock_timeout='10s';
      SELECT public.pay_workbench_candidate_dirty_apply_job_process(
        ${literal(jobIds[0])}::uuid,100
      )::text;
      COMMIT;
    `);
    await waitForLockWait(applicationName);

    run(`
      INSERT INTO public.banking_pay_workbench_jobs(
        id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,
        dedupe_key,candidate_id,payload_json,created_at_utc,updated_at_utc
      ) VALUES (
        ${literal(lateJobId)}::uuid,'WORKBENCH_CANDIDATE_DIRTY_APPLY','QUEUED',-1000,
        clock_timestamp(),0,8,${literal(`${prefix}:JOB:LATE`)},${literal(candidateId)}::uuid,
        jsonb_build_object(
          'candidate_id',${literal(candidateId)},
          'targeted_timesheet_ids',jsonb_build_array(${literal(timesheetIds[2])}),
          'linked_timesheet_ids','[]'::jsonb,'finance_case_ids','[]'::jsonb,
          'reason_latest','DIRTY_TRIGGER:TIMESHEETS:UPDATE',
          'latest_source_change_seq',1001,'source_change_seq',1001,
          'source_change_sequence',1001,'latest_event_at_utc',clock_timestamp()
        ),clock_timestamp(),clock_timestamp()
      );
    `);

    const [leaderDone, blockerDone] = await Promise.all([leader, blocker]);
    assert.equal(leaderDone.status, 0, leaderDone.stderr);
    assert.equal(blockerDone.status, 0, blockerDone.stderr);
    const firstResult = json(leaderDone.stdout);
    assert.equal(firstResult.dirty_apply_cohort_member_count, 2);
    assert.equal(firstResult.dirty_apply_cohort_action, 'COHORT_REISSUED_PENDING_FINALIZATION');
    const firstGeneration = Number(run(`
      SELECT scope_change_generation FROM public.banking_pay_workbench_jobs
      WHERE id=${literal(jobIds[0])}::uuid;
    `).trim());
    const lateBefore = json(run(`
      SELECT jsonb_build_object(
        'status',status,
        'cohort_marker',payload_json ? 'dirty_apply_cohort_contract_version',
        'same_generation',scope_change_generation=${firstGeneration}
      )::text
      FROM public.banking_pay_workbench_jobs WHERE id=${literal(lateJobId)}::uuid;
    `));
    assert.equal(lateBefore.status, 'QUEUED');
    assert.equal(lateBefore.cohort_marker, false);
    assert.equal(lateBefore.same_generation, false);

    const nextResult = json(run(`
      BEGIN;
      UPDATE public.banking_pay_workbench_jobs
      SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp()
      WHERE id=ANY(ARRAY[${jobIds.map(id => `${literal(id)}::uuid`).join(',')}]);
      UPDATE public.banking_pay_workbench_jobs
      SET status='RUNNING',attempt_count=1,started_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp()
      WHERE id=${literal(lateJobId)}::uuid;
      SELECT public.pay_workbench_candidate_dirty_apply_job_process(
        ${literal(lateJobId)}::uuid,100
      )::text;
      COMMIT;
    `));
    assert.equal(nextResult.dirty_apply_cohort_member_count, 1);
    assert.equal(nextResult.dirty_apply_cohort_action, 'COHORT_REISSUED_PENDING_FINALIZATION');
    const lateGeneration = Number(run(`
      SELECT scope_change_generation FROM public.banking_pay_workbench_jobs
      WHERE id=${literal(lateJobId)}::uuid;
    `).trim());
    assert.ok(lateGeneration > firstGeneration);
  } finally {
    cleanupFixture(candidateId);
  }
});
