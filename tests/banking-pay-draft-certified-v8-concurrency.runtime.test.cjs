const assert = require('node:assert/strict');
const { createHash, randomUUID } = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const test = require('node:test');

const container = String(process.env.H2_V8_CONCURRENCY_CONTAINER || '');
const enabled = /^h12-v8-(?:new|release|upgrade|restart)-pg(?:17|18)$/.test(container);
const database = 'banking_modal_v2_test';

function sqlLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function psqlArgs() {
  return [
    'exec', '-i', container,
    'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-qAt',
    '-U', 'postgres', '-d', database
  ];
}

function runSql(sql, { expectFailure = false } = {}) {
  const result = spawnSync('docker', psqlArgs(), {
    input: sql,
    encoding: 'utf8',
    timeout: 30000
  });
  if (expectFailure) {
    assert.notEqual(result.status, 0, 'concurrent call was expected to hit the existing lock budget');
  } else {
    assert.equal(result.status, 0, String(result.stderr || result.stdout || '').slice(0, 3000));
  }
  return result;
}

function runSqlAsync(sql) {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', psqlArgs(), { stdio: ['pipe', 'pipe', 'pipe'] });
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

function parseLastJson(output) {
  const lines = String(output || '').trim().split(/\r?\n/).filter(Boolean);
  assert.ok(lines.length > 0, 'expected a JSON response');
  return JSON.parse(lines.at(-1));
}

test('same-operation concurrency obeys the existing lock budget and replays with zero financial writes', {
  skip: !enabled,
  timeout: 45000
}, async () => {
  const operationId = randomUUID();
  const sessionId = randomUUID();
  const actorId = randomUUID();
  const certificateId = randomUUID();
  const snapshotRunId = randomUUID();
  const digest = createHash('sha256').update(operationId).digest('hex');
  const idempotencyKey = `h2-v8-concurrency-${operationId}`;
  const workerA = `H2_V8_CONCURRENCY_A:${operationId}`;
  const workerB = `H2_V8_CONCURRENCY_B:${operationId}`;

  const setupSql = `
    BEGIN;
    INSERT INTO public.banking_pay_snapshot_runs(
      id,pay_date,week_ending_cutoff,pay_week_start,
      eligibility_from_date,eligibility_to_date,status,is_active
    ) VALUES (
      ${sqlLiteral(snapshotRunId)}::uuid,'2026-09-04','2026-09-06','2026-08-31',
      '2026-08-31','2026-09-06','READY',false
    );
    INSERT INTO public.banking_pay_workbench_sessions(
      id,actor_user_id,pay_date,week_ending_cutoff,status,version,
      progress_counter_version,progress_state,source_snapshot_run_id,
      session_signature,scope_change_generation_target,scope_change_generation_applied,
      scope_change_generation_shadow_checked,authority_fence_generation
    ) VALUES (
      ${sqlLiteral(sessionId)}::uuid,${sqlLiteral(actorId)}::uuid,
      '2026-09-04','2026-09-06','OPEN',1,1,'READY',${sqlLiteral(snapshotRunId)}::uuid,
      repeat('a',64),1,1,1,1
    );
    INSERT INTO public.banking_pay_operations(
      id,operation_type,status,phase,actor_user_id,workbench_session_id,
      idempotency_key,input_json,progress_json
    ) VALUES (
      ${sqlLiteral(operationId)}::uuid,'DRAFT_CREATE','RUNNING','DRAIN_TSFIN',
      ${sqlLiteral(actorId)}::uuid,${sqlLiteral(sessionId)}::uuid,
      ${sqlLiteral(idempotencyKey)},'{}'::jsonb,'{}'::jsonb
    );
    INSERT INTO private.banking_pay_workbench_settled_certificates_v8(
      certificate_uuid,certificate_contract,certification_id,overall_digest_sha256,lifecycle,
      workbench_session_id,session_version,progress_counter_version,progress_state,
      source_snapshot_run_id,session_signature,pay_date,week_ending_cutoff,filters_digest_sha256,
      scope_change_generation_target,scope_change_generation_applied,
      scope_change_generation_shadow_checked,authority_fence_generation,
      publication_count,publications_digest_sha256,scope_total_count,scope_seeded_count,
      scope_ready_count,scope_pending_count,scope_failed_count,line_units_total,line_units_ready,
      line_units_pending,line_units_failed,selected_row_count,selected_page_order,
      selected_page_size_max,selected_pages_fetched,selected_terminal_sentinel_seen,
      selected_sentinel_overflow,all_selected_rows_loaded,
      server_selected_preview_row_ids_provided,server_selected_ids_equal_materialised_selected_ids,
      ready_action_required_blocked_pairwise_disjoint,active_draft_rows_excluded,
      ineligible_rows_excluded,snoozed_rows_excluded,unloaded_selection_gap_count,
      queued_current_job_count,running_current_job_count,unresolved_current_job_count,
      invalid_current_job_pointer_count,historical_terminal_rows_retained,
      historical_terminal_rows_are_not_current_authority,can_create_draft,
      selected_eligible_ready_row_count,blocking_reason_count,gate_digest_sha256,
      selected_constituent_count,selected_canonical_amount_ex_vat_total,selected_partition_count,
      selected_partitions_ordering,policy_contract_version,
      before_policy_projection_digest_sha256,after_policy_projection_digest_sha256,
      policy_digests_equal,execution_recovery_delta_only,forbidden_policy_delta_count,
      no_payment_policy_change,build_id,build_idempotency_key,created_by_user_id,sealed_at_utc,
      filter_binding_mode,filter_context_digest_sha256
    ) VALUES (
      ${sqlLiteral(certificateId)}::uuid,'WORKBENCH_SETTLED_CERTIFICATION_V2',
      'WORKBENCH_SETTLED_CERTIFICATION_V2:'||${sqlLiteral(digest)},${sqlLiteral(digest)},
      'SEALED_CURRENT',${sqlLiteral(sessionId)}::uuid,1,1,'READY',${sqlLiteral(snapshotRunId)}::uuid,
      repeat('a',64),'2026-09-04','2026-09-06',repeat('1',64),1,1,1,1,
      1,repeat('2',64),1,1,1,0,0,1,1,0,0,1,
      'row_ordinal asc, preview_row_id asc',256,1,true,false,true,true,true,true,true,true,true,
      0,0,0,0,0,true,true,true,1,0,repeat('3',64),1,'10.00',1,
      'minimum constituent ordinal asc, candidate_id asc, resolved_pay_channel asc',
      'WORKBENCH_PAYMENT_POLICY_PARITY_V2',repeat('4',64),repeat('4',64),
      true,true,0,true,${sqlLiteral(certificateId)}::uuid,${sqlLiteral(idempotencyKey)},
      ${sqlLiteral(actorId)}::uuid,clock_timestamp(),'EXACT_CERTIFIED_SELECTED_UNIVERSE',repeat('5',64)
    );
    INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
      operation_id,certificate_uuid,pay_channel_scope,constituent_count,partition_count,
      canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
      selected_partitions_digest_sha256,manifest_digest_sha256,freeze_state,frozen_at_utc
    ) VALUES (
      ${sqlLiteral(operationId)}::uuid,${sqlLiteral(certificateId)}::uuid,'ALL',1,1,'10.00',
      repeat('6',64),repeat('7',64),repeat('8',64),'FROZEN',clock_timestamp()
    );
    COMMIT;
  `;

  const cleanupSql = `
    BEGIN;
    DELETE FROM private.banking_pay_draft_frozen_certificate_scopes_v8
      WHERE operation_id=${sqlLiteral(operationId)}::uuid;
    DELETE FROM public.banking_pay_operations WHERE id=${sqlLiteral(operationId)}::uuid;
    DELETE FROM private.banking_pay_workbench_settled_certificates_v8
      WHERE certificate_uuid=${sqlLiteral(certificateId)}::uuid;
    DELETE FROM public.banking_pay_workbench_sessions WHERE id=${sqlLiteral(sessionId)}::uuid;
    DELETE FROM public.banking_pay_snapshot_runs WHERE id=${sqlLiteral(snapshotRunId)}::uuid;
    COMMIT;
  `;

  runSql(setupSql);
  try {
    const holder = runSqlAsync(`
      BEGIN;
      SELECT public.banking_pay_draft_certificate_stage_advance_v8(
        ${sqlLiteral(operationId)}::uuid,${sqlLiteral(workerA)}
      )::text;
      SELECT pg_catalog.pg_sleep(3);
      COMMIT;
    `);

    await new Promise(resolve => setTimeout(resolve, 250));
    const contended = runSql(`
      SELECT public.banking_pay_draft_certificate_stage_advance_v8(
        ${sqlLiteral(operationId)}::uuid,${sqlLiteral(workerB)}
      )::text;
    `, { expectFailure: true });
    assert.match(String(contended.stderr), /canceling statement due to lock timeout/i);

    const first = await holder;
    assert.equal(first.status, 0, first.stderr);
    const firstResult = parseLastJson(first.stdout);
    assert.equal(firstResult.ok, true);
    assert.equal(firstResult.replayed, true);
    assert.equal(firstResult.work_kind, 'CERTIFICATE_STAGE_ALREADY_COMPLETE');

    const retry = parseLastJson(runSql(`
      SELECT public.banking_pay_draft_certificate_stage_advance_v8(
        ${sqlLiteral(operationId)}::uuid,${sqlLiteral(workerB)}
      )::text;
    `).stdout);
    assert.equal(retry.ok, true);
    assert.equal(retry.replayed, true);
    assert.equal(retry.work_kind, 'CERTIFICATE_STAGE_ALREADY_COMPLETE');

    const state = runSql(`
      SELECT jsonb_build_object(
        'operation_rows',(SELECT count(*) FROM public.banking_pay_operations WHERE id=${sqlLiteral(operationId)}::uuid),
        'scope_rows',(SELECT count(*) FROM private.banking_pay_draft_frozen_certificate_scopes_v8 WHERE operation_id=${sqlLiteral(operationId)}::uuid),
        'constituent_rows',(SELECT count(*) FROM private.banking_pay_draft_frozen_constituent_refs_v8 WHERE operation_id=${sqlLiteral(operationId)}::uuid),
        'partition_rows',(SELECT count(*) FROM private.banking_pay_draft_frozen_partition_refs_v8 WHERE operation_id=${sqlLiteral(operationId)}::uuid),
        'candidate_scope_rows',(SELECT count(*) FROM private.banking_pay_draft_frozen_candidate_scopes_v8 WHERE operation_id=${sqlLiteral(operationId)}::uuid),
        'batch_rows',(SELECT count(*) FROM public.pay_batches WHERE source_workbench_session_id=${sqlLiteral(sessionId)}::uuid)
      )::text;
    `);
    assert.deepEqual(parseLastJson(state.stdout), {
      operation_rows: 1,
      scope_rows: 1,
      constituent_rows: 0,
      partition_rows: 0,
      candidate_scope_rows: 0,
      batch_rows: 0
    });
  } finally {
    runSql(cleanupSql);
  }
});

test('two different Create Draft keys for the same Workbench selection serialize and cannot create duplicate operations', {
  skip: !enabled,
  timeout: 45000
}, async () => {
  const sessionId = randomUUID();
  const actorId = randomUUID();
  const snapshotRunId = randomUUID();
  const idempotencyKeyA = `h2-v8-selection-a-${sessionId}`;
  const idempotencyKeyB = `h2-v8-selection-b-${sessionId}`;

  const setupSql = `
    BEGIN;
    INSERT INTO public.banking_pay_snapshot_runs(
      id,pay_date,week_ending_cutoff,pay_week_start,
      eligibility_from_date,eligibility_to_date,status,is_active
    ) VALUES (
      ${sqlLiteral(snapshotRunId)}::uuid,'2026-09-04','2026-09-06','2026-08-31',
      '2026-08-31','2026-09-06','READY',false
    );
    INSERT INTO public.banking_pay_workbench_sessions(
      id,actor_user_id,pay_date,week_ending_cutoff,status,version,
      progress_counter_version,progress_state,source_snapshot_run_id,
      session_signature,scope_change_generation_target,scope_change_generation_applied,
      scope_change_generation_shadow_checked,authority_fence_generation
    ) VALUES (
      ${sqlLiteral(sessionId)}::uuid,${sqlLiteral(actorId)}::uuid,
      '2026-09-04','2026-09-06','OPEN',1,1,'READY',${sqlLiteral(snapshotRunId)}::uuid,
      repeat('a',64),1,1,1,1
    );
    COMMIT;
  `;

  const cleanupSql = `
    BEGIN;
    DELETE FROM public.banking_pay_operations
      WHERE workbench_session_id=${sqlLiteral(sessionId)}::uuid;
    DELETE FROM public.banking_pay_workbench_sessions WHERE id=${sqlLiteral(sessionId)}::uuid;
    DELETE FROM public.banking_pay_snapshot_runs WHERE id=${sqlLiteral(snapshotRunId)}::uuid;
    COMMIT;
  `;

  const startSql = (idempotencyKey) => `
    SELECT row_to_json(started)::text
    FROM public.banking_pay_operation_start(
      'DRAFT_CREATE',
      ${sqlLiteral(actorId)}::uuid,
      ${sqlLiteral(idempotencyKey)},
      ${sqlLiteral(sessionId)}::uuid,
      NULL::uuid,
      NULL::uuid,
      jsonb_build_object('workbench_session_id',${sqlLiteral(sessionId)}),
      '{}'::jsonb
    ) AS started;
  `;

  runSql(setupSql);
  try {
    const holder = runSqlAsync(`
      BEGIN;
      ${startSql(idempotencyKeyA)}
      SELECT pg_catalog.pg_sleep(3);
      COMMIT;
    `);

    await new Promise(resolve => setTimeout(resolve, 250));
    const contended = runSql(startSql(idempotencyKeyB), { expectFailure: true });
    assert.match(String(contended.stderr), /canceling statement due to lock timeout/i);

    const first = await holder;
    assert.equal(first.status, 0, first.stderr);
    const firstResult = parseLastJson(first.stdout);
    assert.equal(firstResult.operation_type, 'DRAFT_CREATE');
    assert.equal(firstResult.workbench_session_id, sessionId);
    assert.equal(firstResult.idempotency_key, idempotencyKeyA);
    assert.equal(firstResult.is_existing, false);

    const secondResult = parseLastJson(runSql(startSql(idempotencyKeyB)).stdout);
    assert.equal(secondResult.operation_id, firstResult.operation_id);
    assert.equal(secondResult.idempotency_key, idempotencyKeyA);
    assert.equal(secondResult.is_existing, true);

    const state = parseLastJson(runSql(`
      SELECT jsonb_build_object(
        'operation_rows',(SELECT count(*) FROM public.banking_pay_operations
          WHERE workbench_session_id=${sqlLiteral(sessionId)}::uuid),
        'active_operation_rows',(SELECT count(*) FROM public.banking_pay_operations
          WHERE workbench_session_id=${sqlLiteral(sessionId)}::uuid
            AND upper(status) IN ('QUEUED','RUNNING','WAITING','WAITING_AUTHORISATION',
              'WAITING_AUTHORIZATION','WAITING_PROVIDER','CONTINUING','WAITING_RETRY')),
        'batch_rows',(SELECT count(*) FROM public.pay_batches
          WHERE source_workbench_session_id=${sqlLiteral(sessionId)}::uuid),
        'second_key_rows',(SELECT count(*) FROM public.banking_pay_operations
          WHERE idempotency_key=${sqlLiteral(idempotencyKeyB)})
      )::text;
    `).stdout);
    assert.deepEqual(state, {
      operation_rows: 1,
      active_operation_rows: 1,
      batch_rows: 0,
      second_key_rows: 0
    });
  } finally {
    runSql(cleanupSql);
  }
});
