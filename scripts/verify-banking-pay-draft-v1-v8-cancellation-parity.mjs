import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';

const ACTOR_ID = '10000000-0000-4000-8000-000000000001';
const DATABASE_SOURCE = 'banking_modal_v2_test';
const REPLACEMENT_SOURCE = path.resolve(
  'supabase/repeatable/04092026_2118_banking_pay_multi_candidate_cancel_continuation_v1.sql'
);
const CANCEL_SAFE_SOURCE = path.resolve(
  'supabase/repeatable/19072026_1816_cancel_refresh_supersede_finance_dirty.sql'
);
const CORRECTION_CHUNK_SOURCE = path.resolve(
  'supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql'
);
const CANCELLATION_COMPLETION_SOURCE = path.resolve(
  'supabase/repeatable/04092026_2350_banking_pay_cancellation_completion_v1.sql'
);
const ONE_CANDIDATE_SELECTION_INTEGRITY_SOURCE = path.resolve(
  'supabase/repeatable/05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql'
);
const RESERVATION_EVIDENCE_PRECEDENCE_SOURCE = path.resolve(
  'supabase/repeatable/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql'
);
const BANK_EVENT_CLASSIFICATION_CONSTRAINT_SOURCE = path.resolve(
  'supabase/migrations/05092026_1330_banking_pay_bank_event_movement_classification_v1.sql'
);
const NO_MONEY_UNWIND_RESULT_ARITY_SOURCE = path.resolve(
  'supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql'
);
const RECONCILIATION_ENVELOPE_SOURCE = path.resolve(
  'supabase/migrations/06082026_0407_banking_pay_reconciliation_envelope_v2.sql'
);
const SCHEDULED_LOCAL_PREPARE_RUNTIME_SOURCE = path.resolve(
  'tests/04092026_2340_banking_pay_scheduled_due_local_prepare_runtime.sql'
);
const matrices = [
  {
    name: 'PG17',
    v1: {
      container: 'h12-v1-oracle-pg17',
      database: 'h2_cancel_v1_pg17',
      batches: {
        PAYE: '87f0b451-eec9-426a-bb38-c54109013328',
        UMBRELLA: 'fa228931-41d5-47b9-bb1a-aa1731d9e997'
      }
    },
    v8: {
      container: 'h12-v8-restart-pg17',
      database: 'h2_cancel_v8_pg17',
      batches: {
        PAYE: 'fda06a34-168f-468f-98b8-e9f3fe9d029d',
        UMBRELLA: '664390b3-6547-4c28-9af2-4baf0c6d7cdb'
      }
    }
  },
  {
    name: 'PG18',
    v1: {
      container: 'h12-v1-oracle-pg18',
      database: 'h2_cancel_v1_pg18',
      batches: {
        PAYE: '18ff6bd4-58ed-4133-8627-437f69b41275',
        UMBRELLA: '538acc8f-5213-442b-aa76-af3974ae717f'
      }
    },
    v8: {
      container: 'h12-v8-restart-pg18',
      database: 'h2_cancel_v8_pg18',
      batches: {
        PAYE: 'd8a00679-fd2f-4def-8a1f-f0350ca597a4',
        UMBRELLA: '1d97a6a4-706f-4ec7-a8a4-98f03a7468c2'
      }
    }
  }
];

function runDocker(container, args, { allowFailure = false, input = undefined, interactive = false } = {}) {
  const startedAt = performance.now();
  const result = spawnSync('docker', ['exec', ...(interactive ? ['-i'] : []), container, ...args], {
    input,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
    windowsHide: true
  });
  const elapsedMs = Number((performance.now() - startedAt).toFixed(3));
  if (!allowFailure && result.status !== 0) {
    throw new Error([
      `docker exec failed in ${container}`,
      result.stdout,
      result.stderr
    ].filter(Boolean).join('\n'));
  }
  return { ...result, elapsedMs };
}

function replaceExactlyOnce(source, before, after, label) {
  assert.equal(source.split(before).length - 1, 1, `${label} splice changed`);
  return source.replace(before, after);
}

function establishLocalNotSent(target, batchId, scheduleKind = 'SCHEDULED') {
  assert.ok(['IMMEDIATE', 'SCHEDULED'].includes(scheduleKind), `unknown schedule kind ${scheduleKind}`);
  const original = fs.readFileSync(SCHEDULED_LOCAL_PREPARE_RUNTIME_SOURCE, 'utf8').replaceAll('\r\n', '\n');
  const dueClaim = [
    "  v_claim_result := public.pay_batches_claim_due_scheduled(50, now() + interval '2 days');",
    "  v_claim_replay_result := public.pay_batches_claim_due_scheduled(50, now() + interval '2 days');"
  ].join('\n');
  const noClaim = [
    "  v_claim_result := jsonb_build_object('ok',true,'code','NOT_INVOKED_FOR_CANCELLATION_FIXTURE','claimed_count',0,'skipped_count',0,'operations','[]'::jsonb);",
    '  v_claim_replay_result := v_claim_result;'
  ].join('\n');
  const operationRelease = /\n  v_result := public\.banking_pay_operation_release_lease\([\s\S]*?\n  END IF;\n/;
  assert.equal(original.split(dueClaim).length - 1, 1, 'scheduled cancellation due-claim splice changed');
  assert.equal((original.match(operationRelease) || []).length, 1, 'scheduled cancellation operation-release splice changed');
  assert.match(original, /ROLLBACK;\s*$/);
  // The reusable rollback fixture deliberately performs every step in one
  // transaction. The real Worker commits the schedule/link transitions,
  // lets the bounded Workbench owner finalise them, and only then releases
  // PAYMENT_EXECUTE as COMPLETE. Split only that final release in this
  // disposable integration driver so the sealed execution chain observes
  // the same committed causal order.
  let fixture = original
    .replace(dueClaim, noClaim)
    .replace(operationRelease, `
  v_result := pg_catalog.jsonb_build_object(
    'ok', true,
    'status', 'RUNNING',
    'code', 'H2_OPERATION_RELEASE_DEFERRED_FOR_COMMITTED_WORKBENCH_DRAIN'
  );
`)
    .replace(/ROLLBACK;\s*$/, 'COMMIT;\n');
  if (scheduleKind === 'IMMEDIATE') {
    fixture = replaceExactlyOnce(
      fixture,
      "      'schedule_kind', 'SCHEDULED',\n      'scheduled_at_utc', (now() + interval '1 day')::text,",
      "      'schedule_kind', 'IMMEDIATE',\n      'scheduled_at_utc', NULL::text,",
      'operation input schedule kind'
    );
    fixture = replaceExactlyOnce(
      fixture,
      "  v_auth_result := public.pay_batch_auth_start(\n    v_batch,\n    'SCHEDULED',\n    now() + interval '1 day',",
      "  v_auth_result := public.pay_batch_auth_start(\n    v_batch,\n    'IMMEDIATE',\n    NULL::timestamptz,",
      'authorisation schedule kind'
    );
    fixture = replaceExactlyOnce(
      fixture,
      "  v_schedule_result := public.pay_batch_schedule(\n    v_batch,\n    'SCHEDULED',\n    now() + interval '1 day',",
      "  v_schedule_result := public.pay_batch_schedule(\n    v_batch,\n    'IMMEDIATE',\n    NULL::timestamptz,",
      'payment schedule kind'
    );
  }
  const result = runDocker(target.container, [
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1',
    '-v', `h2_batch_id=${batchId}`,
    '-v', 'h2_evidence_mode=LOCAL_ONLY'
  ], { input: fixture, interactive: true });
  const marker = '__H2_EXECUTE_PREP__';
  const line = result.stdout.split(/\r?\n/).find((value) => value.startsWith(marker));
  assert.ok(line, `scheduled cancellation fixture marker missing: ${result.stdout.slice(-2000)}`);
  const payload = JSON.parse(line.slice(marker.length));
  assert.equal(payload.prepare?.ok, true, JSON.stringify(payload));
  assert.equal(payload.batch_prepare?.ready, true, JSON.stringify(payload));
  assert.equal(payload.auth?.state, 'AUTHORISED', JSON.stringify(payload));
  assert.equal(payload.schedule?.batch_status, 'SCHEDULED', JSON.stringify(payload));
  assert.equal(payload.schedule?.schedule_kind, scheduleKind, JSON.stringify(payload));
  assert.equal(payload.provider_attempt_count, 0, JSON.stringify(payload));
  assert.equal(payload.provider_event_count, 0, JSON.stringify(payload));
  assert.ok(payload.canonical_transfer_classifier.every(
    (row) => row.evidence_classification === 'SCHEDULED_LOCAL_NOT_SENT'
      && row.has_provider_submission_evidence === false
  ), JSON.stringify(payload.canonical_transfer_classifier));
  const deferredRelease = queryJson(target, `
    SELECT public.banking_pay_operation_release_lease(
      p_operation_id := '${payload.operation_id}'::uuid,
      p_lease_owner := 'h2-scheduled-local-prepare',
      p_release_state := 'MORE_WORK',
      p_run_after_delay_seconds := 0,
      p_progress_patch_json := pg_catalog.jsonb_build_object('phase', 'SCHEDULE_PAYMENT'),
      p_result_patch_json := NULL::jsonb,
      p_error_json := NULL::jsonb,
      p_resume_reason := 'AWAITING_COMMITTED_WORKBENCH_TRANSITION_FINALISATION',
      p_actor_user_id := '${ACTOR_ID}'::uuid
    )::text
  `);
  assert.equal(deferredRelease.value.ok, true, JSON.stringify(deferredRelease.value));
  const workbenchDrain = drainCancellationWorkbenchSourceBuilds(target, batchId);
  assert.equal(workbenchDrain.currentness?.all_terminal_current, true, JSON.stringify(workbenchDrain));
  const reclaimed = queryJson(target, `
    SELECT pg_catalog.to_jsonb(claim_row)::text
    FROM public.banking_pay_operation_claim_next(
      p_operation_id := '${payload.operation_id}'::uuid,
      p_actor_user_id := '${ACTOR_ID}'::uuid,
      p_lock_owner := 'h2-scheduled-local-prepare',
      p_lock_seconds := 60,
      p_allow_backend_runner_owned := true,
      p_operation_types := ARRAY['PAYMENT_EXECUTE']::text[]
    ) AS claim_row
    LIMIT 1
  `);
  assert.equal(reclaimed.value.claimed, true, JSON.stringify(reclaimed.value));
  const release = queryJson(target, `
    SELECT public.banking_pay_operation_release_lease(
      p_operation_id := '${payload.operation_id}'::uuid,
      p_lease_owner := 'h2-scheduled-local-prepare',
      p_release_state := 'COMPLETE',
      p_run_after_delay_seconds := 0,
      p_progress_patch_json := pg_catalog.jsonb_build_object('phase', 'COMPLETE'),
      p_result_patch_json := pg_catalog.jsonb_build_object(
        'ok', true,
        'status_text', 'Payment execution operation complete.',
        'pay_batch_schedule', '${JSON.stringify(payload.schedule).replaceAll("'", "''")}'::jsonb
      ),
      p_error_json := NULL::jsonb,
      p_resume_reason := 'OPERATION_COMPLETE',
      p_actor_user_id := '${ACTOR_ID}'::uuid
    )::text
  `);
  assert.equal(release.value.ok, true, JSON.stringify(release.value));
  assert.equal(release.value.status, 'COMPLETE', JSON.stringify(release.value));
  const sealedChain = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'contract_version', progress_json#>>'{execution_unsent_overlay_chain_v2,contract_version}',
      'candidate_count', COALESCE((progress_json#>>'{execution_unsent_overlay_chain_v2,candidate_count}')::integer, 0),
      'closed_candidate_count', (
        SELECT pg_catalog.count(*)::integer
        FROM pg_catalog.jsonb_each(COALESCE(
          progress_json#>'{execution_unsent_overlay_chain_v2,candidates}', '{}'::jsonb
        )) AS candidate(candidate_id, evidence)
        WHERE COALESCE((candidate.evidence->>'closed')::boolean, false) IS TRUE
      ),
      'candidate_diagnostics', (
        SELECT COALESCE(pg_catalog.jsonb_object_agg(
          candidate.candidate_id,
          pg_catalog.jsonb_build_object(
            'closed', candidate.evidence->'closed',
            'rejection_reason', candidate.evidence->'rejection_reason',
            'transition_count', candidate.evidence->'transition_count',
            'link_transition_count', candidate.evidence->'link_transition_count',
            'schedule_transition_count', candidate.evidence->'schedule_transition_count'
          ) ORDER BY candidate.candidate_id
        ), '{}'::jsonb)
        FROM pg_catalog.jsonb_each(COALESCE(
          progress_json#>'{execution_unsent_overlay_chain_v2,candidates}', '{}'::jsonb
        )) AS candidate(candidate_id, evidence)
      )
    )::text
    FROM public.banking_pay_operations
    WHERE id = '${payload.operation_id}'::uuid
  `).value;
  assert.equal(sealedChain.contract_version, 'EXECUTION_UNSENT_OVERLAY_CHAIN_V2', JSON.stringify(sealedChain));
  assert.equal(sealedChain.candidate_count, payload.canonical_transfer_classifier.length, JSON.stringify(sealedChain));
  if (sealedChain.closed_candidate_count !== sealedChain.candidate_count) {
    assert.equal(sealedChain.closed_candidate_count, 0, JSON.stringify(sealedChain));
    assert.ok(Object.values(sealedChain.candidate_diagnostics || {}).every(
      (candidate) => candidate.closed === false
        && candidate.rejection_reason === 'EXECUTION_OVERLAY_CHAIN_INCOMPLETE'
        && candidate.transition_count === 1
        && candidate.link_transition_count === 1
        && candidate.schedule_transition_count === 0
    ), JSON.stringify(sealedChain));
  }
  return {
    schedule_kind: scheduleKind,
    batch_status: payload.schedule.batch_status,
    transfer_count: payload.transfer_count,
    provider_attempt_count: payload.provider_attempt_count,
    provider_event_count: payload.provider_event_count,
    classification: 'SCHEDULED_LOCAL_NOT_SENT',
    workbench_drain_iteration_count: workbenchDrain.iteration_count,
    execution_overlay_chain_candidate_count: sealedChain.candidate_count,
    execution_overlay_chain_closed_candidate_count: sealedChain.closed_candidate_count,
    execution_overlay_chain_candidate_diagnostics: sealedChain.candidate_diagnostics,
    elapsed_ms: Number((result.elapsedMs + deferredRelease.elapsedMs + reclaimed.elapsedMs + release.elapsedMs).toFixed(3))
  };
}

function establishProviderFailedNoMoney(target, batchId, scheduleKind) {
  const preparation = establishLocalNotSent(target, batchId, scheduleKind);
  const transfers = queryJson(target, `
    SELECT COALESCE(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'id', transfer_row.id,
        'provider', transfer_row.rail_provider,
        'rail_env', transfer_row.rail_env,
        'request_id', transfer_row.request_id,
        'amount', transfer_row.amount
      ) ORDER BY transfer_row.id
    ), '[]'::jsonb)::text
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = '${batchId}'::uuid
  `).value;
  assert.ok(Array.isArray(transfers) && transfers.length > 0, JSON.stringify(transfers));

  const eventResults = [];
  for (const [ordinal, transfer] of transfers.entries()) {
    const idempotencyKey = `h2-terminal-no-money:${target.database}:${batchId}:${scheduleKind.toLowerCase()}:${ordinal + 1}`;
    const ingested = queryJson(target, `
      SELECT public.pay_bank_event_ingest(
        pg_catalog.jsonb_build_object(
          'pay_batch_id', '${batchId}',
          'pay_bank_transfer_id', '${transfer.id}',
          'provider_key', '${String(transfer.provider).replaceAll("'", "''")}',
          'provider_event_id', '${idempotencyKey}',
          'provider_event_key', '${idempotencyKey}',
          'provider_reference', '${String(transfer.request_id || transfer.id).replaceAll("'", "''")}',
          'provider_state', 'FAILED',
          'normalised_state', 'FAILED',
          'event_source', 'PROVIDER_POLL',
          'provider_event_transport', 'PROVIDER_POLL',
          'rail_env', '${String(transfer.rail_env).replaceAll("'", "''")}',
          'provider_failure_reason_code', 'H2_ROLLBACK_PROVIDER_REJECTED',
          'no_payment_made', true,
          'terminal_no_money', true,
          'suppress_auto_unwind', true,
          'amount', '${transfer.amount}',
          'currency', 'GBP',
          'idempotency_key', '${idempotencyKey}',
          'raw_payload', pg_catalog.jsonb_build_object(
            'source', 'H2_DISPOSABLE_DATABASE_ONLY',
            'status', 'FAILED',
            'no_payment_made', true,
            'terminal_no_money', true
          )
        ),
        '${ACTOR_ID}'::uuid,
        pg_catalog.jsonb_build_object('suppress_auto_unwind', true)
      )::text
    `);
    assert.equal(ingested.value.ok, true, JSON.stringify(ingested.value));
    assert.equal(ingested.value.mapped, true, JSON.stringify(ingested.value));
    assert.equal(ingested.value.normalised_state, 'FAILED', JSON.stringify(ingested.value));
    assert.equal(ingested.value.classification, 'PROVIDER_FAILED_NO_MONEY', JSON.stringify(ingested.value));
    assert.equal(ingested.value.release_eligible, true, JSON.stringify(ingested.value));
    assert.equal(ingested.value.auto_release_request_prepared, false, JSON.stringify(ingested.value));
    assert.equal(ingested.value.requires_user_action, true, JSON.stringify(ingested.value));
    assert.equal(ingested.value.money_movement_classification?.is_terminal_no_money, true, JSON.stringify(ingested.value));
    assert.equal(ingested.value.money_movement_classification?.is_final_money_moved, false, JSON.stringify(ingested.value));
    eventResults.push({
      transfer_id: transfer.id,
      classification: ingested.value.classification,
      normalised_state: ingested.value.normalised_state,
      release_eligible: ingested.value.release_eligible,
      correction_disposition: ingested.value.correction_disposition,
      elapsed_ms: ingested.elapsedMs
    });
  }

  const replay = queryJson(target, `
    SELECT public.pay_bank_event_ingest(
      pg_catalog.jsonb_build_object(
        'pay_batch_id', '${batchId}',
        'pay_bank_transfer_id', '${transfers[0].id}',
        'provider_key', '${String(transfers[0].provider).replaceAll("'", "''")}',
        'provider_event_id', 'h2-terminal-no-money:${target.database}:${batchId}:${scheduleKind.toLowerCase()}:1',
        'provider_event_key', 'h2-terminal-no-money:${target.database}:${batchId}:${scheduleKind.toLowerCase()}:1',
        'provider_reference', '${String(transfers[0].request_id || transfers[0].id).replaceAll("'", "''")}',
        'provider_state', 'FAILED',
        'normalised_state', 'FAILED',
        'event_source', 'PROVIDER_POLL',
        'provider_event_transport', 'PROVIDER_POLL',
        'rail_env', '${String(transfers[0].rail_env).replaceAll("'", "''")}',
        'provider_failure_reason_code', 'H2_ROLLBACK_PROVIDER_REJECTED',
        'no_payment_made', true,
        'terminal_no_money', true,
        'suppress_auto_unwind', true,
        'amount', '${transfers[0].amount}',
        'currency', 'GBP',
        'idempotency_key', 'h2-terminal-no-money:${target.database}:${batchId}:${scheduleKind.toLowerCase()}:1',
        'raw_payload', pg_catalog.jsonb_build_object(
          'source', 'H2_DISPOSABLE_DATABASE_ONLY',
          'status', 'FAILED',
          'no_payment_made', true,
          'terminal_no_money', true
        )
      ),
      '${ACTOR_ID}'::uuid,
      pg_catalog.jsonb_build_object('suppress_auto_unwind', true)
    )::text
  `);
  assert.equal(replay.value.ok, true, JSON.stringify(replay.value));
  assert.equal(replay.value.idempotent, true, JSON.stringify(replay.value));

  const classified = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'event_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_bank_transfer_events AS event_row
        WHERE event_row.pay_batch_id = '${batchId}'::uuid
      ),
      'transfer_count', pg_catalog.count(*)::integer,
      'failed_no_money_count', pg_catalog.count(*) FILTER (
        WHERE classified.evidence_classification = 'PROVIDER_FAILED_NO_MONEY'
      )::integer,
      'all_terminal_no_money', pg_catalog.bool_and(
        classified.evidence_classification = 'PROVIDER_FAILED_NO_MONEY'
        AND movement.movement_json->>'classification' = 'PROVIDER_FAILED_NO_MONEY'
        AND classified.is_terminal_or_completed IS TRUE
      )
    )::text
    FROM public.pay_bank_transfer_execution_classify(
      '${batchId}'::uuid, 'ALL', NULL::uuid, true, 'CURRENT_PAYMENT_STATUS'
    ) AS classified
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_movement_classify(
        '${batchId}'::uuid,
        pg_catalog.jsonb_build_object(
          'scope_type', 'TRANSFER',
          'pay_bank_transfer_ids', pg_catalog.jsonb_build_array(classified.pay_bank_transfer_id::text),
          'source_context', 'BANK_EVENT_INGEST',
          'requested_action', 'NO_MONEY_UNWIND_AND_RECALCULATE'
        )
      ) AS movement_json
    ) AS movement
  `).value;
  assert.equal(classified.event_count, transfers.length, JSON.stringify(classified));
  assert.equal(classified.transfer_count, transfers.length, JSON.stringify(classified));
  assert.equal(classified.failed_no_money_count, transfers.length, JSON.stringify(classified));
  assert.equal(classified.all_terminal_no_money, true, JSON.stringify(classified));

  return {
    ...preparation,
    classification: 'PROVIDER_FAILED_NO_MONEY',
    simulated_inbound_provider_event_count: eventResults.length,
    event_replay_idempotent: true,
    event_results: eventResults,
    outbound_provider_actions: 0
  };
}

function execSql(target, sql) {
  const result = runDocker(target.container, [
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1', '-Atq', '-c', sql
  ]);
  const output = result.stdout.trim();
  return { output, elapsedMs: result.elapsedMs };
}

function queryJson(target, sql) {
  const { output, elapsedMs } = execSql(target, sql);
  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  assert.equal(lines.length, 1, `expected one JSON row, got ${lines.length}: ${output.slice(0, 1000)}`);
  return { value: JSON.parse(lines[0]), elapsedMs };
}

function hasDiagnosticToken(error, expectedToken) {
  const tokens = new Set(
    String(error?.message || error || '')
      .toUpperCase()
      .split(/[^A-Z0-9_]+/)
      .filter(Boolean)
  );
  return tokens.has(expectedToken);
}

function cloneDatabase(target) {
  assert.match(target.database, /^h2_cancel_(?:v1|v8)_pg(?:17|18)$/);
  const dumpPath = `/tmp/${target.database}.dump`;
  runDocker(target.container, ['dropdb', '-U', 'postgres', '--if-exists', target.database]);
  runDocker(target.container, ['rm', '-f', dumpPath]);
  try {
    // PostgREST intentionally keeps the source database connected. A logical
    // copy avoids terminating that valid task-owned service merely to create a
    // disposable parity database.
    runDocker(target.container, ['pg_dump', '-U', 'postgres', '-d', DATABASE_SOURCE, '-Fc', '-f', dumpPath]);
    runDocker(target.container, ['createdb', '-U', 'postgres', target.database]);
    runDocker(target.container, ['pg_restore', '-U', 'postgres', '-d', target.database, '--no-owner', '--exit-on-error', dumpPath]);
  } finally {
    runDocker(target.container, ['rm', '-f', dumpPath], { allowFailure: true });
  }
}

function dropDatabase(target) {
  assert.match(target.database, /^h2_cancel_(?:v1|v8)_pg(?:17|18)$/);
  runDocker(target.container, ['dropdb', '-U', 'postgres', '--if-exists', target.database], { allowFailure: true });
}

function normalizeCancellationScaleFinancials(target) {
  // The persisted scale oracle uses zero worked units plus a separate £1
  // adjustment. Its original Timesheet financial row also carried a synthetic
  // £1 parent total, which is not the sum of its zero physical rate buckets and
  // is therefore correctly rejected by the current rate-authority guard during
  // a fresh Workbench build. Correct only that disposable fixture inconsistency;
  // the selected adjustment and every frozen Draft artifact remain unchanged.
  return queryJson(target, `
    WITH batch_timesheets AS (
      SELECT DISTINCT item_row.timesheet_id
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS candidate_row
        ON candidate_row.id = item_row.pay_batch_candidate_id
      WHERE candidate_row.pay_batch_id = ANY(ARRAY[
        '${target.batches.PAYE}'::uuid,
        '${target.batches.UMBRELLA}'::uuid
      ])
        AND item_row.timesheet_id IS NOT NULL
    ), corrected_timesheets AS (
      UPDATE public.timesheets AS timesheet_row
      SET worked_end_iso = timesheet_row.worked_start_iso,
          worked_minutes = 0,
          actual_schedule_json = COALESCE(timesheet_row.actual_schedule_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'end', COALESCE(
                timesheet_row.actual_schedule_json->>'start',
                substring(timesheet_row.worked_start_iso::text from 'T([0-9]{2}:[0-9]{2})')
              ),
              'break_minutes', 0
            ),
          updated_at = pg_catalog.clock_timestamp()
      WHERE timesheet_row.timesheet_id IN (SELECT timesheet_id FROM batch_timesheets)
        AND COALESCE(timesheet_row.worked_minutes, 0) <> 0
      RETURNING timesheet_row.timesheet_id
    ), corrected_financials AS (
      UPDATE public.timesheets_financials AS financial_row
      SET total_pay_ex_vat = 0,
          total_charge_ex_vat = 0,
          margin_ex_vat = 0,
          worked_end_iso = financial_row.worked_start_iso,
          actual_schedule_json = COALESCE(financial_row.actual_schedule_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'end', COALESCE(
                financial_row.actual_schedule_json->>'start',
                substring(financial_row.worked_start_iso::text from 'T([0-9]{2}:[0-9]{2})')
              ),
              'break_minutes', 0
            ),
          actual_minutes_by_day_json = '{}'::jsonb,
          updated_at = pg_catalog.clock_timestamp()
      WHERE financial_row.timesheet_id IN (SELECT timesheet_id FROM batch_timesheets)
        AND financial_row.is_current IS TRUE
        AND financial_row.is_stale IS NOT TRUE
        AND COALESCE(financial_row.total_hours, 0) = 0
        AND COALESCE(financial_row.hours_day, 0) = 0
        AND COALESCE(financial_row.hours_night, 0) = 0
        AND COALESCE(financial_row.hours_sat, 0) = 0
        AND COALESCE(financial_row.hours_sun, 0) = 0
        AND COALESCE(financial_row.hours_bh, 0) = 0
        AND COALESCE(financial_row.expenses_pay_ex_vat, 0) = 0
        AND COALESCE(financial_row.mileage_pay_ex_vat, 0) = 0
        AND COALESCE(financial_row.additional_pay_ex_vat, 0) = 0
        AND COALESCE(financial_row.travel_pay_ex_vat, 0) = 0
        AND COALESCE(financial_row.accommodation_pay_ex_vat, 0) = 0
        AND COALESCE(financial_row.other_pay_ex_vat, 0) = 0
        AND (
          COALESCE(financial_row.total_pay_ex_vat, 0) <> 0
          OR COALESCE(financial_row.total_charge_ex_vat, 0) <> 0
          OR COALESCE(financial_row.margin_ex_vat, 0) <> 0
        )
      RETURNING financial_row.timesheet_id
    )
    SELECT pg_catalog.jsonb_build_object(
      'corrected_zero_duration_timesheet_count',
        (SELECT pg_catalog.count(*)::integer FROM corrected_timesheets),
      'corrected_zero_unit_parent_total_count',
        (SELECT pg_catalog.count(*)::integer FROM corrected_financials)
    )::text
  `).value;
}

function clearPreexistingCancellationWorkbenchJobs(target) {
  // The persisted V1/V8 oracle databases intentionally retain unrelated
  // queued fixture history. This disposable clone isolates the cancellation
  // boundary so only jobs caused by the cancellation under test are drained.
  // Succeeded/failed audit history is preserved.
  return queryJson(target, `
    WITH batch_candidates AS (
      SELECT candidate_row.candidate_id
      FROM public.pay_batch_candidates AS candidate_row
      WHERE candidate_row.pay_batch_id = ANY(ARRAY[
        '${target.batches.PAYE}'::uuid,
        '${target.batches.UMBRELLA}'::uuid
      ])
    ), removed AS (
      DELETE FROM public.banking_pay_workbench_jobs AS job_row
      WHERE job_row.candidate_id IN (SELECT candidate_id FROM batch_candidates)
        AND job_row.status IN ('QUEUED', 'RUNNING')
      RETURNING job_row.id
    )
    SELECT pg_catalog.jsonb_build_object(
      'removed_active_fixture_job_count', pg_catalog.count(*)::integer
    )::text
    FROM removed
  `).value;
}

function installRepositoryReconciliationEnvelope(target) {
  const containerPath = `/tmp/${path.basename(RECONCILIATION_ENVELOPE_SOURCE)}`;
  const copied = spawnSync('docker', ['cp', RECONCILIATION_ENVELOPE_SOURCE, `${target.container}:${containerPath}`], {
    encoding: 'utf8',
    windowsHide: true
  });
  if (copied.status !== 0) {
    throw new Error([copied.stdout, copied.stderr].filter(Boolean).join('\n'));
  }
  try {
    runDocker(target.container, [
      'env',
      'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
      'psql', '-U', 'postgres', '-d', target.database,
      '-X', '-v', 'ON_ERROR_STOP=1', '-f', containerPath
    ]);
  } finally {
    runDocker(target.container, ['rm', '-f', containerPath], { allowFailure: true });
  }
}

function installReplacement(target) {
  const containerPath = `/tmp/${path.basename(REPLACEMENT_SOURCE)}`;
  const copied = spawnSync('docker', ['cp', REPLACEMENT_SOURCE, `${target.container}:${containerPath}`], {
    encoding: 'utf8',
    windowsHide: true
  });
  if (copied.status !== 0) {
    throw new Error([copied.stdout, copied.stderr].filter(Boolean).join('\n'));
  }
  try {
    runDocker(target.container, [
      'env',
      'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
      'psql', '-U', 'postgres', '-d', target.database,
      '-X', '-v', 'ON_ERROR_STOP=1', '-f', containerPath
    ]);
  } finally {
    runDocker(target.container, ['rm', '-f', containerPath], { allowFailure: true });
  }
}

function installOneCandidateSelectionIntegrity(target) {
  const containerPath = `/tmp/${path.basename(ONE_CANDIDATE_SELECTION_INTEGRITY_SOURCE)}`;
  const copied = spawnSync('docker', ['cp', ONE_CANDIDATE_SELECTION_INTEGRITY_SOURCE, `${target.container}:${containerPath}`], {
    encoding: 'utf8',
    windowsHide: true
  });
  if (copied.status !== 0) {
    throw new Error([copied.stdout, copied.stderr].filter(Boolean).join('\n'));
  }
  try {
    runDocker(target.container, [
      'env',
      'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
      'psql', '-U', 'postgres', '-d', target.database,
      '-X', '-v', 'ON_ERROR_STOP=1', '-f', containerPath
    ]);
  } finally {
    runDocker(target.container, ['rm', '-f', containerPath], { allowFailure: true });
  }
}

function installReservationEvidencePrecedence(target) {
  const containerPath = `/tmp/${path.basename(RESERVATION_EVIDENCE_PRECEDENCE_SOURCE)}`;
  const copied = spawnSync('docker', ['cp', RESERVATION_EVIDENCE_PRECEDENCE_SOURCE, `${target.container}:${containerPath}`], {
    encoding: 'utf8',
    windowsHide: true
  });
  if (copied.status !== 0) {
    throw new Error([copied.stdout, copied.stderr].filter(Boolean).join('\n'));
  }
  try {
    runDocker(target.container, [
      'env',
      'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
      'psql', '-U', 'postgres', '-d', target.database,
      '-X', '-v', 'ON_ERROR_STOP=1', '-f', containerPath
    ]);
  } finally {
    runDocker(target.container, ['rm', '-f', containerPath], { allowFailure: true });
  }
}

function installBankEventClassificationConstraint(target) {
  const containerPath = '/tmp/05092026_1330_banking_pay_bank_event_movement_classification_v1.sql';
  const copied = spawnSync('docker', ['cp', BANK_EVENT_CLASSIFICATION_CONSTRAINT_SOURCE, `${target.container}:${containerPath}`], {
    encoding: 'utf8',
    windowsHide: true
  });
  if (copied.status !== 0) {
    throw new Error([copied.stdout, copied.stderr].filter(Boolean).join('\n'));
  }
  runDocker(target.container, [
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1', '-f', containerPath
  ]);
}

function installNoMoneyUnwindResultArity(target) {
  const candidateSource = fs.readFileSync(NO_MONEY_UNWIND_RESULT_ARITY_SOURCE, 'utf8');
  assert.match(candidateSource, /v_result := jsonb_build_object\(/);
  assert.match(candidateSource, /'blockers', '\[\]'::jsonb\s*\) \|\| jsonb_build_object\(/);
  const result = spawnSync('docker', [
    'exec', '-i', target.container,
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1'
  ], {
    input: candidateSource,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
    windowsHide: true
  });
  if (result.status !== 0) {
    throw new Error([result.stdout, result.stderr].filter(Boolean).join('\n'));
  }
}

function installDiagnosticCancelSafeWrapper(target) {
  const originalSource = fs.readFileSync(CANCEL_SAFE_SOURCE, 'utf8');
  const originalFragment = [
    "          'currentness_reason',v_candidate_currentness->>'currentness_reason'",
    '        )::text;'
  ].join('\n');
  const diagnosticFragment = [
    "          'currentness_reason',v_candidate_currentness->>'currentness_reason',",
    "          'physical_currentness',v_candidate_currentness,",
    "          'republication_result',v_republication_result,",
    "          'enqueue_result',v_enqueue_result,",
    "          'post_commit_authority',v_post_commit_authority,",
    "          'preceding_scope_proven',v_preceding_scope_proven",
    '        )::text;'
  ].join('\n');
  const occurrenceCount = originalSource.split(originalFragment).length - 1;
  assert.equal(occurrenceCount, 1, 'diagnostic wrapper patch point must remain unique');
  const diagnosticSource = originalSource.replace(originalFragment, diagnosticFragment);
  const result = spawnSync('docker', [
    'exec', '-i', target.container,
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1'
  ], {
    input: diagnosticSource,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
    windowsHide: true
  });
  if (result.status !== 0) {
    throw new Error([result.stdout, result.stderr].filter(Boolean).join('\n'));
  }
}

function installDiagnosticCorrectionRoute(target) {
  const originalSource = fs.readFileSync(CORRECTION_CHUNK_SOURCE, 'utf8');
  const originalFragment = [
    '      ELSE',
    '        v_refresh_result := public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1('
  ].join('\n');
  const diagnosticFragment = [
    '      ELSE',
    "        RAISE EXCEPTION 'H2_CORRECTION_ROUTE_HANDOFF_DIAGNOSTIC'",
    "          USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(",
    "            'post_commit_authorities_v3',v_post_commit_authorities_v3,",
    "            'route_authorities_v3',v_route_authorities_v3,",
    "            'committed_route_input',v_committed_route_input",
    '          )::text;',
    '        v_refresh_result := public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1('
  ].join('\n');
  const occurrenceCount = originalSource.split(originalFragment).length - 1;
  assert.equal(occurrenceCount, 1, 'process route diagnostic patch point must remain unique');
  const diagnosticSource = originalSource.replace(originalFragment, diagnosticFragment);
  const result = spawnSync('docker', [
    'exec', '-i', target.container,
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1'
  ], {
    input: diagnosticSource,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
    windowsHide: true
  });
  if (result.status !== 0) {
    throw new Error([result.stdout, result.stderr].filter(Boolean).join('\n'));
  }
}

function installCorrectionRouteHandoffCandidate(target) {
  const originalSource = fs.readFileSync(CORRECTION_CHUNK_SOURCE, 'utf8');
  const originalFragment = [
    "          'candidate_ids', v_refresh_candidate_ids,",
    "          'changed_pay_batch_item_ids', v_refresh_pay_batch_item_ids,",
    "          'maximum_candidate_count', 100",
    '        )'
  ].join('\n');
  const candidateFragment = [
    "          'candidate_ids', v_refresh_candidate_ids,",
    "          'changed_pay_batch_item_ids', v_refresh_pay_batch_item_ids,",
    "          'maximum_candidate_count', 100,",
    "          'defer_complex_enqueue', true,",
    "          'post_commit_authorities_v3', v_route_authorities_v3",
    '        )'
  ].join('\n');
  const occurrenceCount = originalSource.split(originalFragment).length - 1;
  assert.equal(occurrenceCount, 1, 'route handoff candidate patch point must remain unique');
  const candidateSource = originalSource.replace(originalFragment, candidateFragment);
  const result = spawnSync('docker', [
    'exec', '-i', target.container,
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1'
  ], {
    input: candidateSource,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
    windowsHide: true
  });
  if (result.status !== 0) {
    throw new Error([result.stdout, result.stderr].filter(Boolean).join('\n'));
  }
}

function installCancellationCompletionAuditCandidate(target) {
  const candidateSource = fs.readFileSync(CANCELLATION_COMPLETION_SOURCE, 'utf8');
  assert.match(candidateSource, /CREATE OR REPLACE FUNCTION public\.pay_payment_correction_process_chunk\(/);
  assert.match(candidateSource, /'defer_complex_enqueue', true,/);
  assert.match(candidateSource, /'post_commit_authorities_v3', v_route_authorities_v3/);
  assert.match(candidateSource, /PERFORM public\.pay_payment_cancel_finalise_metadata_v1\(/);
  const result = spawnSync('docker', [
    'exec', '-i', target.container,
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', target.database,
    '-X', '-v', 'ON_ERROR_STOP=1'
  ], {
    input: candidateSource,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
    windowsHide: true
  });
  if (result.status !== 0) {
    throw new Error([result.stdout, result.stderr].filter(Boolean).join('\n'));
  }
}

function establishProductionShapedWorkbenchSource(target, batchId) {
  execSql(target, `
    DO $h2_production_source$
    DECLARE
      v_row record;
      v_work_date date;
    BEGIN
      FOR v_row IN
        SELECT DISTINCT
          item_row.timesheet_id,
          item_row.source_ref,
          candidate_row.candidate_id,
          candidate_row.pay_batch_id,
          batch_row.source_workbench_session_id AS session_id,
          batch_row.batch_kind_fixed AS pay_channel,
          preview_row.id AS preview_row_id,
          preview_row.row_json
        FROM public.pay_batches AS batch_row
        JOIN public.pay_batch_candidates AS candidate_row
          ON candidate_row.pay_batch_id = batch_row.id
        JOIN public.pay_batch_items AS item_row
          ON item_row.pay_batch_candidate_id = candidate_row.id
        JOIN public.banking_pay_workbench_preview_rows AS preview_row
          ON preview_row.session_id = batch_row.source_workbench_session_id
         AND preview_row.candidate_id = candidate_row.candidate_id
         AND preview_row.row_key = item_row.source_ref
        WHERE batch_row.id = '${batchId}'::uuid
          AND item_row.timesheet_id IS NOT NULL
          AND COALESCE(item_row.is_voided, false) IS FALSE
        ORDER BY candidate_row.candidate_id, item_row.timesheet_id
      LOOP
        SELECT COALESCE(
          NULLIF(financial_row.actual_schedule_json->>'date', '')::date,
          financial_row.worked_start_iso::date,
          timesheet_row.week_ending_date
        )
        INTO STRICT v_work_date
        FROM public.timesheets AS timesheet_row
        JOIN public.timesheets_financials AS financial_row
          ON financial_row.timesheet_id = timesheet_row.timesheet_id
         AND financial_row.is_current IS TRUE
         AND financial_row.is_stale IS FALSE
        WHERE timesheet_row.timesheet_id = v_row.timesheet_id
          AND timesheet_row.is_current IS TRUE;

        UPDATE public.timesheets_financials AS financial_row
        SET hours_day = 1,
            hours_night = 0,
            hours_sat = 0,
            hours_sun = 0,
            hours_bh = 0,
            pay_day = 1,
            pay_night = 0,
            pay_sat = 0,
            pay_sun = 0,
            pay_bh = 0,
            charge_day = 1,
            charge_night = 0,
            charge_sat = 0,
            charge_sun = 0,
            charge_bh = 0,
            total_hours = 1,
            total_pay_ex_vat = 1,
            total_charge_ex_vat = 1,
            margin_ex_vat = 0,
            worked_start_iso = (v_work_date::text || 'T08:00:00Z')::timestamptz,
            worked_end_iso = (v_work_date::text || 'T09:00:00Z')::timestamptz,
            actual_schedule_json = pg_catalog.jsonb_build_object(
              'date', v_work_date::text,
              'start', '08:00',
              'end', '09:00',
              'break_minutes', 0
            ),
            actual_minutes_by_day_json = pg_catalog.jsonb_build_object(v_work_date::text, 60),
            updated_at = pg_catalog.clock_timestamp()
        WHERE financial_row.timesheet_id = v_row.timesheet_id
          AND financial_row.is_current IS TRUE
          AND financial_row.is_stale IS FALSE;

        UPDATE public.timesheets AS timesheet_row
        SET worked_start_iso = (v_work_date::text || 'T08:00:00Z')::timestamptz,
            worked_end_iso = (v_work_date::text || 'T09:00:00Z')::timestamptz,
            actual_schedule_json = pg_catalog.jsonb_build_object(
              'date', v_work_date::text,
              'start', '08:00',
              'end', '09:00',
              'break_minutes', 0
            ),
            updated_at = pg_catalog.clock_timestamp()
        WHERE timesheet_row.timesheet_id = v_row.timesheet_id
          AND timesheet_row.is_current IS TRUE;

        -- Preserve the existing adjustment row and its ADJUSTMENT_CODE key.
        -- The scale-only source is promoted to a genuine £1 worked-time
        -- entitlement plus the separate £1 adjustment. The frozen Draft has
        -- selected only the adjustment; after cancellation, a fresh Workbench
        -- build must expose both still-unpaid source entitlements.
      END LOOP;
    END;
    $h2_production_source$;
  `);
}

function establishCurrentWorkbenchAuthority(target, batchId) {
  execSql(target, `
    DO $h2_current_source$
    DECLARE
      v_row record;
      v_job_id uuid;
      v_source_count integer;
      v_source_digest text;
      v_source_identity_digest text;
      v_preview_identity_digest text;
      v_source_publication_id uuid;
    BEGIN
      UPDATE public.settings_defaults
      SET banking_pay_source_publication_identity_write_v1_enabled = true,
          banking_pay_source_publication_identity_enforce_v1_enabled = true
      WHERE id = 1;

      FOR v_row IN
        SELECT
          batch_row.source_workbench_session_id AS session_id,
          session_row.version AS session_version,
          session_row.source_snapshot_run_id,
          candidate_row.candidate_id,
          scope_row.scope_ordinal,
          registry_row.current_source_change_seq,
          registry_row.dirty_generation,
          build_row.id AS economic_build_id,
          build_row.source_build_run_id
        FROM public.pay_batches AS batch_row
        JOIN public.pay_batch_candidates AS candidate_row
          ON candidate_row.pay_batch_id = batch_row.id
        JOIN public.banking_pay_workbench_sessions AS session_row
          ON session_row.id = batch_row.source_workbench_session_id
        JOIN public.banking_pay_workbench_session_scope AS scope_row
          ON scope_row.session_id = session_row.id
         AND scope_row.candidate_id = candidate_row.candidate_id
        JOIN private.banking_pay_workbench_candidate_scope_registry AS registry_row
          ON registry_row.candidate_id = candidate_row.candidate_id
        JOIN private.banking_pay_workbench_economic_builds AS build_row
          ON build_row.session_id = session_row.id
         AND build_row.candidate_id = candidate_row.candidate_id
         AND build_row.source_build_run_id = scope_row.certified_preview_publication_source_build_run_id
        WHERE batch_row.id = '${batchId}'::uuid
        ORDER BY candidate_row.candidate_id
      LOOP
        v_source_publication_id := private.pay_workbench_source_publication_identity_v1(
          v_row.session_id,
          v_row.candidate_id,
          v_row.session_version,
          v_row.current_source_change_seq,
          v_row.source_build_run_id
        );

        -- The scale fixture predates the publisher's million-row Candidate
        -- ordinal namespace. Align the synthetic live preview to the exact
        -- current publisher contract before testing post-cancel reappearance.
        UPDATE public.banking_pay_workbench_preview_rows AS preview_row
        SET row_ordinal = (v_row.scope_ordinal * 1000000) + source_row.source_ordinal,
            updated_at_utc = pg_catalog.clock_timestamp()
        FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        WHERE preview_row.session_id = v_row.session_id
          AND preview_row.candidate_id = v_row.candidate_id
          AND source_row.session_id = preview_row.session_id
          AND source_row.candidate_id = preview_row.candidate_id
          AND preview_row.row_json->>'source_line_id' = source_row.id::text;

        UPDATE public.banking_pay_workbench_candidate_source_lines AS source_row
        SET source_change_seq = v_row.current_source_change_seq,
            line_key = preview_row.row_key,
            source_row_json = (
              preview_row.row_json - ARRAY[
                'actual_refresh_scope_kind',
                'post_draft_effective_draftable',
                'post_draft_effective_is_ready_for_draft',
                'post_draft_effective_presentation_section',
                'post_draft_effective_readiness_state',
                'post_draft_effective_selection_allowed',
                'post_draft_overlay_active',
                'post_draft_overlay_applied',
                'post_draft_overlay_at_utc',
                'post_draft_overlay_operation_type',
                'post_draft_overlay_pay_batch_id',
                'post_draft_unavailable',
                'post_draft_unavailable_reason',
                'selection_identity_digest'
              ]::text[]
            ) || pg_catalog.jsonb_build_object(
              'line_key', preview_row.row_key,
              'source_change_seq', v_row.current_source_change_seq,
              'source_ordinal', source_row.source_ordinal,
              'selected', false,
              'selection_state', 'UNSELECTED'
            ),
            source_publication_id = v_source_publication_id,
            updated_at_utc = pg_catalog.clock_timestamp()
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        WHERE source_row.session_id = v_row.session_id
          AND source_row.candidate_id = v_row.candidate_id
          AND preview_row.session_id = source_row.session_id
          AND preview_row.candidate_id = source_row.candidate_id
          AND preview_row.row_json->>'source_line_id' = source_row.id::text;

        SELECT
          pg_catalog.count(*)::integer,
          pg_catalog.md5(COALESCE(pg_catalog.string_agg(
            pg_catalog.md5(source_row.source_row_json::text),
            '' ORDER BY source_row.source_ordinal
          ), '')),
          pg_catalog.md5(COALESCE(pg_catalog.string_agg(
            public.pay_workbench_preview_section_from_line_json(source_row.source_row_json)
              || E'\\x1f' || source_row.line_key || E'\\x1f' || source_row.source_ordinal::text,
            E'\\x1e' ORDER BY source_row.source_ordinal,
              public.pay_workbench_preview_section_from_line_json(source_row.source_row_json),
              source_row.line_key
          ), ''))
        INTO v_source_count, v_source_digest, v_source_identity_digest
        FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        WHERE source_row.session_id = v_row.session_id
          AND source_row.candidate_id = v_row.candidate_id
          AND source_row.session_version = v_row.session_version
          AND source_row.source_change_seq = v_row.current_source_change_seq
          AND source_row.source_build_run_id = v_row.source_build_run_id
          AND source_row.source_publication_id = v_source_publication_id
          AND source_row.status = 'CURRENT';

        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          preview_row.section || E'\\x1f' || preview_row.row_key || E'\\x1f'
            || (preview_row.row_ordinal - (v_row.scope_ordinal * 1000000))::text,
          E'\\x1e' ORDER BY
            preview_row.row_ordinal - (v_row.scope_ordinal * 1000000),
            preview_row.section,
            preview_row.row_key
        ), ''))
        INTO v_preview_identity_digest
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        WHERE preview_row.session_id = v_row.session_id
          AND preview_row.candidate_id = v_row.candidate_id
          AND preview_row.session_version = v_row.session_version
          AND preview_row.status = 'READY';

        IF v_source_count <= 0
           OR v_source_identity_digest IS DISTINCT FROM v_preview_identity_digest THEN
          RAISE EXCEPTION 'H2_CANCEL_CURRENT_SOURCE_FIXTURE_IDENTITY_MISMATCH';
        END IF;

        v_job_id := extensions.gen_random_uuid();
        INSERT INTO public.banking_pay_workbench_jobs(
          id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
          dedupe_key, snapshot_run_id, session_id, candidate_id, payload_json,
          completed_at_utc, scope_change_generation, economic_build_id,
          private_stage, private_cursor_kind, private_stage_version
        ) VALUES (
          v_job_id, 'WORKBENCH_CANDIDATE_SOURCE_BUILD', 'SUCCEEDED', 100,
          pg_catalog.clock_timestamp(), 1, 8,
          'H2_CANCEL_CURRENT_SOURCE:' || v_row.candidate_id::text,
          v_row.source_snapshot_run_id, v_row.session_id, v_row.candidate_id,
          pg_catalog.jsonb_build_object(
            'fixture', 'H2_CANCEL_CURRENT_SOURCE',
            'source_change_seq', v_row.current_source_change_seq,
            'scope_change_generation', v_row.dirty_generation
          ),
          pg_catalog.clock_timestamp(), v_row.dirty_generation,
          v_row.economic_build_id, 'COMPLETE', 'COMPLETE', 1
        );

        UPDATE private.banking_pay_workbench_economic_builds AS build_row
        SET source_change_seq = v_row.current_source_change_seq,
            source_job_id = v_job_id,
            canonical_count = v_source_count,
            canonical_digest = v_source_digest,
            attestation_json = COALESCE(build_row.attestation_json, '{}'::jsonb)
              || pg_catalog.jsonb_build_object(
                'effect_plan_sealed', true,
                'effect_plan_digest', pg_catalog.md5('H2_CANCEL_EFFECT_PLAN:' || v_row.candidate_id::text),
                'observed_finance_effect_digest', pg_catalog.md5('H2_CANCEL_OBSERVED_EFFECT:' || v_row.candidate_id::text)
              )
        WHERE build_row.id = v_row.economic_build_id;

        UPDATE public.banking_pay_workbench_session_candidate_state AS state_row
        SET status = 'READY',
            source_change_seq = v_row.current_source_change_seq,
            session_version = v_row.session_version,
            pending_job_id = NULL::uuid,
            last_error_json = NULL::jsonb,
            updated_at_utc = pg_catalog.clock_timestamp()
        WHERE state_row.session_id = v_row.session_id
          AND state_row.candidate_id = v_row.candidate_id;

        UPDATE public.banking_pay_workbench_session_scope AS scope_row
        SET status = 'READY',
            seeded = true,
            dirty = false,
            pending_job_id = NULL::uuid,
            error_json = NULL::jsonb,
            certified_preview_publication_required = true,
            certified_preview_publication_parity_ok = true,
            certified_preview_publication_session_version = v_row.session_version,
            certified_preview_publication_source_change_seq = v_row.current_source_change_seq,
            certified_preview_publication_source_build_run_id = v_row.source_build_run_id,
            certified_preview_publication_source_publication_id = v_source_publication_id,
            certified_preview_publication_attestation_json = pg_catalog.jsonb_build_object(
              'attestation_version', 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3',
              'contract_version', '3',
              'semantic_contract_version', 'READY_TO_PAY_SEMANTIC_V2',
              'authority_kind', 'BOUNDED_FULL_SOURCE_BUILD',
              'final_state', 'READY',
              'semantic_ready', true,
              'parity_complete', true,
              'invalid_selectable_row_count', 0,
              'candidate_ready_amount', (
                SELECT COALESCE(pg_catalog.sum((source_row.source_row_json->>'amount_ex_vat')::numeric), 0)
                FROM public.banking_pay_workbench_candidate_source_lines AS source_row
                WHERE source_row.session_id = v_row.session_id
                  AND source_row.candidate_id = v_row.candidate_id
                  AND source_row.session_version = v_row.session_version
                  AND source_row.source_change_seq = v_row.current_source_change_seq
                  AND source_row.status = 'CURRENT'
              ),
              'session_id', v_row.session_id,
              'candidate_id', v_row.candidate_id,
              'session_version', v_row.session_version,
              'source_change_seq', v_row.current_source_change_seq,
              'source_build_run_id', v_row.source_build_run_id,
              'source_publication_id', v_source_publication_id,
              'economic_build_id', v_row.economic_build_id,
              'completion_job_id', v_job_id,
              'source_row_count', v_source_count,
              'preview_row_count', v_source_count,
              'source_digest', v_source_digest,
              'source_identity_digest', v_source_identity_digest,
              'preview_identity_digest', v_preview_identity_digest,
              'semantic_proof_digest', pg_catalog.md5('H2_CANCEL_SEMANTIC_PROOF:' || v_row.candidate_id::text)
            ),
            certified_preview_publication_attested_at_utc = pg_catalog.clock_timestamp(),
            updated_at_utc = pg_catalog.clock_timestamp()
        WHERE scope_row.session_id = v_row.session_id
          AND scope_row.candidate_id = v_row.candidate_id;
      END LOOP;
    END;
    $h2_current_source$;
  `);

  // Prove that the synthetic authority is accepted by the unchanged certified
  // publisher before the cancellation path relies on its duplicate-replay
  // repair branch.
  execSql(target, `
    DO $h2_current_publish$
    DECLARE
      v_row record;
      v_result jsonb;
    BEGIN
      FOR v_row IN
        SELECT
          batch_row.source_workbench_session_id AS session_id,
          session_row.version AS session_version,
          candidate_row.candidate_id,
          scope_row.certified_preview_publication_source_change_seq AS source_change_seq,
          scope_row.certified_preview_publication_source_build_run_id AS source_build_run_id,
          scope_row.certified_preview_publication_attestation_json AS attestation
        FROM public.pay_batches AS batch_row
        JOIN public.pay_batch_candidates AS candidate_row
          ON candidate_row.pay_batch_id = batch_row.id
        JOIN public.banking_pay_workbench_sessions AS session_row
          ON session_row.id = batch_row.source_workbench_session_id
        JOIN public.banking_pay_workbench_session_scope AS scope_row
          ON scope_row.session_id = session_row.id
         AND scope_row.candidate_id = candidate_row.candidate_id
        WHERE batch_row.id = '${batchId}'::uuid
        ORDER BY candidate_row.candidate_id
      LOOP
        v_result := private.pay_workbench_publish_certified_source_preview_v1(
          v_row.session_id,
          v_row.candidate_id,
          (v_row.attestation->>'economic_build_id')::uuid,
          v_row.source_build_run_id,
          v_row.source_change_seq,
          v_row.session_version,
          (v_row.attestation->>'completion_job_id')::uuid,
          'CANDIDATE_FULL_LIVE',
          '[]'::jsonb,
          '[]'::jsonb,
          pg_catalog.jsonb_build_object(
            'contract_version', 3,
            'semantic_contract_version', 'READY_TO_PAY_SEMANTIC_V2',
            'authority_kind', 'BOUNDED_FULL_SOURCE_BUILD',
            'invocation_kind', 'DUPLICATE_REPLAY_REPAIR',
            'final_state', 'READY'
          )
        );
        IF COALESCE((v_result->>'ok')::boolean, false) IS NOT TRUE
           OR COALESCE((v_result->>'parity_complete')::boolean, false) IS NOT TRUE THEN
          RAISE EXCEPTION 'H2_CANCEL_CURRENT_SOURCE_PUBLISHER_REJECTED:%', v_result;
        END IF;
      END LOOP;
    END;
    $h2_current_publish$;
  `);

  const currentness = queryJson(target, `
    SELECT private.pay_workbench_candidate_physical_currentness_page_v1(
      batch_row.source_workbench_session_id,
      ARRAY_AGG(candidate_row.candidate_id ORDER BY candidate_row.candidate_id),
      'TERMINAL_CURRENT',
      pg_catalog.jsonb_build_object('contract_version', 1, 'allow_active_owner', false)
    )::text
    FROM public.pay_batches AS batch_row
    JOIN public.pay_batch_candidates AS candidate_row
      ON candidate_row.pay_batch_id = batch_row.id
    WHERE batch_row.id = '${batchId}'::uuid
    GROUP BY batch_row.source_workbench_session_id
  `).value;
  assert.equal(currentness.all_terminal_current, true, JSON.stringify(currentness));
  assert.equal(currentness.terminal_current_count, currentness.candidate_count, JSON.stringify(currentness));
  return currentness;
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])]));
}

function sha256(value) {
  return crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
}

function claim(target, operationId, workerId) {
  return queryJson(target, `
    SELECT pg_catalog.to_jsonb(claim_row)::text
    FROM public.banking_pay_operation_claim_next(
      p_operation_id := '${operationId}'::uuid,
      p_actor_user_id := '${ACTOR_ID}'::uuid,
      p_lock_owner := '${workerId}',
      p_lock_seconds := 60,
      p_allow_backend_runner_owned := true,
      p_operation_types := ARRAY['PAYMENT_CORRECTION']::text[]
    ) AS claim_row
    LIMIT 1
  `);
}

function readOperation(target, operationId) {
  return queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'status', status,
      'phase', phase,
      'runner_state', runner_state,
      'lease_owner', lease_owner,
      'requires_user_action', requires_user_action
    )::text
    FROM public.banking_pay_operations
    WHERE id = '${operationId}'::uuid
  `);
}

function readFailureDiagnostics(target, requestId, operationId) {
  if (!requestId || !operationId) return null;
  return queryJson(target, `
    WITH work_facts AS (
      SELECT
        work_row.id AS work_item_id,
        work_row.candidate_id,
        work_row.pay_batch_candidate_id,
        work_row.status,
        work_row.attempt_count,
        work_row.selection_hash,
        member_row.candidate_scope_hash,
        member_row.active_item_count AS expected_active_item_count,
        member_row.active_amount AS expected_active_amount,
        member_row.pay_batch_item_ids AS expected_item_ids,
        candidate_row.net_bank_amount AS current_net_bank_amount,
        COALESCE(item_fact.active_item_count, 0)::integer AS current_active_item_count,
        COALESCE(item_fact.active_item_ids, ARRAY[]::uuid[]) AS current_active_item_ids,
        work_row.result_json->'blocker' AS blocker,
        work_row.result_json->>'classification' AS classification,
        work_row.result_json->'same_request_continuation_proof' AS continuation_proof
      FROM public.pay_payment_correction_work_items AS work_row
      LEFT JOIN public.pay_payment_correction_request_candidates AS member_row
        ON member_row.correction_request_id = work_row.correction_request_id
       AND member_row.pay_batch_candidate_id = work_row.pay_batch_candidate_id
      LEFT JOIN public.pay_batch_candidates AS candidate_row
        ON candidate_row.id = work_row.pay_batch_candidate_id
      LEFT JOIN LATERAL (
        SELECT
          pg_catalog.count(*) FILTER (
            WHERE COALESCE(item_row.is_voided, false) IS FALSE
              AND item_row.item_type <> 'DEBT_CREATED'
          )::integer AS active_item_count,
          pg_catalog.array_agg(item_row.id ORDER BY item_row.id) FILTER (
            WHERE COALESCE(item_row.is_voided, false) IS FALSE
          ) AS active_item_ids
        FROM public.pay_batch_items AS item_row
        WHERE item_row.pay_batch_candidate_id = work_row.pay_batch_candidate_id
      ) AS item_fact ON true
      WHERE work_row.correction_request_id = '${requestId}'::uuid
    )
    SELECT pg_catalog.jsonb_build_object(
      'request_status', request_row.status,
      'request_code', request_row.plan_json->'final_result'->>'code',
      'operation_status', operation_row.status,
      'operation_phase', operation_row.phase,
      'batch_status', batch_row.status,
      'active_item_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_batch_items AS item_row
        JOIN public.pay_batch_candidates AS candidate_row
          ON candidate_row.id = item_row.pay_batch_candidate_id
        WHERE candidate_row.pay_batch_id = request_row.pay_batch_id
          AND COALESCE(item_row.is_voided, false) IS FALSE
      ),
      'voided_item_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_batch_items AS item_row
        JOIN public.pay_batch_candidates AS candidate_row
          ON candidate_row.id = item_row.pay_batch_candidate_id
        WHERE candidate_row.pay_batch_id = request_row.pay_batch_id
          AND COALESCE(item_row.is_voided, false) IS TRUE
      ),
      'applied_work_item_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS work_row
        WHERE work_row.correction_request_id = request_row.id
          AND work_row.status = 'APPLIED'
      ),
      'total_work_item_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS work_row
        WHERE work_row.correction_request_id = request_row.id
      ),
      'applied_correction_item_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_items AS correction_item
        WHERE correction_item.correction_request_id = request_row.id
          AND correction_item.status = 'APPLIED'
      ),
      'provider_attempt_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.banking_pay_operation_provider_attempts AS provider_attempt
        WHERE provider_attempt.operation_id = operation_row.id
      ),
      'provider_event_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_bank_transfer_events AS provider_event
        WHERE provider_event.pay_batch_id = request_row.pay_batch_id
      ),
      'work_items', COALESCE((
        SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'work_item_id', fact.work_item_id,
          'candidate_id', fact.candidate_id,
          'pay_batch_candidate_id', fact.pay_batch_candidate_id,
          'status', fact.status,
          'attempt_count', fact.attempt_count,
          'selection_hash_matches', fact.selection_hash IS NOT DISTINCT FROM fact.candidate_scope_hash,
          'expected_active_item_count', fact.expected_active_item_count,
          'current_active_item_count', fact.current_active_item_count,
          'expected_active_amount', fact.expected_active_amount,
          'current_net_bank_amount', fact.current_net_bank_amount,
          'expected_item_ids', fact.expected_item_ids,
          'current_active_item_ids', fact.current_active_item_ids,
          'blocker', fact.blocker,
          'classification', fact.classification,
          'same_request_continuation_proof', fact.continuation_proof
        ) ORDER BY fact.pay_batch_candidate_id)
        FROM work_facts AS fact
      ), '[]'::jsonb)
    )::text
    FROM public.pay_payment_correction_requests AS request_row
    JOIN public.banking_pay_operations AS operation_row
      ON operation_row.id = '${operationId}'::uuid
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = request_row.pay_batch_id
    WHERE request_row.id = '${requestId}'::uuid
  `).value;
}

function readPostCommitAuthorityDiagnostics(target, requestId, operationId) {
  return queryJson(target, `
    WITH authority_scope AS (
      SELECT request_row.id AS correction_request_id,
             operation_row.id AS operation_id,
             batch_row.source_workbench_session_id AS session_id,
             pg_catalog.array_agg(work_row.id ORDER BY work_row.id) AS work_item_ids
      FROM public.pay_payment_correction_requests AS request_row
      JOIN public.banking_pay_operations AS operation_row
        ON operation_row.id = '${operationId}'::uuid
       AND operation_row.pay_batch_id = request_row.pay_batch_id
      JOIN public.pay_batches AS batch_row ON batch_row.id = request_row.pay_batch_id
      JOIN public.pay_payment_correction_work_items AS work_row
        ON work_row.correction_request_id = request_row.id
       AND work_row.status = 'APPLIED'
      WHERE request_row.id = '${requestId}'::uuid
      GROUP BY request_row.id, operation_row.id, batch_row.source_workbench_session_id
    )
    SELECT private.pay_workbench_correction_post_commit_authority_page_v1(
      authority_scope.correction_request_id,
      authority_scope.operation_id,
      authority_scope.session_id,
      authority_scope.work_item_ids,
      pg_catalog.jsonb_build_object('owner', 'h2_cancellation_diagnostic')
    )::text
    FROM authority_scope
  `).value;
}

function readCancellationWorkbenchCurrentness(target, batchId, allowActiveOwner = false) {
  return queryJson(target, `
    SELECT private.pay_workbench_candidate_physical_currentness_page_v1(
      batch_row.source_workbench_session_id,
      ARRAY_AGG(candidate_row.candidate_id ORDER BY candidate_row.candidate_id),
      'TERMINAL_CURRENT',
      pg_catalog.jsonb_build_object('contract_version', 1, 'allow_active_owner', ${allowActiveOwner ? 'true' : 'false'})
    )::text
    FROM public.pay_batches AS batch_row
    JOIN public.pay_batch_candidates AS candidate_row
      ON candidate_row.pay_batch_id = batch_row.id
    WHERE batch_row.id = '${batchId}'::uuid
    GROUP BY batch_row.source_workbench_session_id
  `).value;
}

function drainCancellationWorkbenchSourceBuilds(target, batchId) {
  const scope = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'session_id', batch_row.source_workbench_session_id,
      'candidate_ids', pg_catalog.jsonb_agg(candidate_row.candidate_id::text ORDER BY candidate_row.candidate_id)
    )::text
    FROM public.pay_batches AS batch_row
    JOIN public.pay_batch_candidates AS candidate_row ON candidate_row.pay_batch_id = batch_row.id
    WHERE batch_row.id = '${batchId}'::uuid
    GROUP BY batch_row.source_workbench_session_id
  `).value;
  const workerId = `h2-cancel-workbench-${target.database}`;
  const laneId = `h2-cancel-workbench-lane-${target.database}`;
  const normalJobTypesSql = `ARRAY[
    'WORKBENCH_SESSION_SCOPE_SEED',
    'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'WORKBENCH_SESSION_CLONE_REBASE',
    'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
    'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
    'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
    'CONTRACT_CLIENT_DIRTY_FANOUT'
  ]::text[]`;
  const safeSteps = [];
  for (let iteration = 0; iteration < 80; iteration += 1) {
    const currentness = readCancellationWorkbenchCurrentness(target, batchId, false);
    if (currentness.all_terminal_current === true) {
      return { currentness, safe_steps: safeSteps, iteration_count: iteration };
    }
    let claimedThisIteration = 0;
    let scanProgressThisIteration = 0;
    let dirtyProgressThisIteration = 0;
    for (const candidateId of scope.candidate_ids) {
      // The production aggregate Worker drains the priority dirty lane before
      // attempting ordinary stage work. Cancellation-created dirty jobs are
      // deliberately global (session_id NULL), so the faithful scheduled-worker
      // invocation must not apply a session filter here. Candidate filtering
      // keeps this rollback fixture bounded to the Draft under test.
      const dirtyDrain = queryJson(target, `
        SELECT public.pay_workbench_worker_drain_chunk_revalidated_v1(
          1,
          clock_timestamp(),
          NULL::uuid,
          '${candidateId}'::uuid,
          ${normalJobTypesSql},
          '${workerId}:NORMAL',
          180
        )::text
      `).value;
      assert.equal(dirtyDrain.ok, true, JSON.stringify({
        code: 'H2_CANCEL_WORKBENCH_DIRTY_DRAIN_FAILED',
        candidate_id: candidateId,
        result: dirtyDrain
      }));
      const dirtyMadeProgress = dirtyDrain.dirty_priority_made_progress === true
        || Number(dirtyDrain.dirty_priority_jobs_processed || 0) > 0;
      if (dirtyMadeProgress) dirtyProgressThisIteration += 1;
      safeSteps.push({
        candidate_id: candidateId,
        route: 'DIRTY_PRIORITY',
        ok: dirtyDrain.ok === true,
        processed: Number(dirtyDrain.dirty_priority_jobs_processed || 0),
        remaining: Number(dirtyDrain.dirty_priority_jobs_remaining || 0),
        result_code: dirtyDrain.stop_reason || null
      });

      const claim = queryJson(target, `
        SELECT public.pay_workbench_source_build_attempt_claim_start_v1(
          '${workerId}', '${laneId}', 25, NULL::timestamptz,
          '${scope.session_id}'::uuid, '${candidateId}'::uuid
        )::text
      `).value;
      if (claim.claimed !== true) {
        if (claim.scan_progress === true) {
          scanProgressThisIteration += 1;
          safeSteps.push({
            candidate_id: candidateId,
            private_stage: null,
            attempt_number: null,
            ok: claim.ok === true,
            processed: false,
            stage_status: null,
            result_code: claim.result_code || 'CLAIM_SCAN_PROGRESS',
            has_more: true,
            continuation_enqueued: false
          });
        }
        continue;
      }
      claimedThisIteration += 1;
      const execution = queryJson(target, `
        SELECT public.pay_workbench_source_build_attempt_execute_v1(
          '${claim.job_id}'::uuid,
          '${claim.build_id}'::uuid,
          '${claim.private_stage}',
          '${claim.attempt_id}'::uuid,
          '${claim.attempt_nonce}'::uuid,
          '${workerId}',
          '${laneId}'
        )::text
      `).value;
      const failureEvidence = execution.ok === true ? null : queryJson(target, `
        SELECT pg_catalog.jsonb_build_object(
          'code', job_row.last_error_json->>'code',
          'first_divergent_code', job_row.last_error_json->'first_divergent_cause'->>'code',
          'latest_observed_code', job_row.last_error_json->'latest_observed_failure'->>'code',
          'message', job_row.last_error_json->'first_divergent_cause'->>'message',
          'detail', job_row.last_error_json->'first_divergent_cause'->>'detail'
        )::text
        FROM public.banking_pay_workbench_jobs AS job_row
        WHERE job_row.id = '${claim.job_id}'::uuid
      `).value;
      safeSteps.push({
        candidate_id: candidateId,
        route: 'SOURCE_BUILD',
        private_stage: claim.private_stage,
        attempt_number: claim.attempt_number,
        ok: execution.ok === true,
        processed: execution.processed === true,
        stage_status: execution.stage_status || null,
        result_code: execution.result_code || null,
        has_more: execution.has_more === true,
        continuation_enqueued: execution.continuation_enqueued === true,
        failure_result: execution.ok === true ? null : (execution.failure_result || null),
        failure_evidence: failureEvidence
      });
      assert.equal(execution.ok, true, JSON.stringify(safeSteps.at(-1)));
    }
    assert.ok(dirtyProgressThisIteration > 0 || claimedThisIteration > 0 || scanProgressThisIteration > 0, JSON.stringify({
      code: 'H2_CANCEL_WORKBENCH_DRAIN_NO_PROGRESS',
      currentness,
      safe_steps: safeSteps
    }));
  }
  throw new Error(JSON.stringify({
    code: 'H2_CANCEL_WORKBENCH_DRAIN_LIMIT_EXCEEDED',
    maximum_iteration_count: 80,
    currentness: readCancellationWorkbenchCurrentness(target, batchId, true),
    safe_step_count: safeSteps.length,
    safe_step_tail: safeSteps.slice(-12)
  }));
}

function release(target, operationId, workerId, operation, phaseCalls) {
  if (operation.lease_owner !== workerId) return null;
  const terminal = operation.status === 'COMPLETE' || operation.phase === 'COMPLETE';
  const result = queryJson(target, `
    SELECT public.banking_pay_operation_release_lease(
      p_operation_id := '${operationId}'::uuid,
      p_lease_owner := '${workerId}',
      p_release_state := '${terminal ? 'COMPLETE' : 'MORE_WORK'}',
      p_run_after_delay_seconds := 0,
      p_progress_patch_json := pg_catalog.jsonb_build_object(
        'phase', '${operation.phase}',
        'h2_cancellation_parity_fixture', true
      ),
      p_result_patch_json := NULL::jsonb,
      p_error_json := NULL::jsonb,
      p_resume_reason := '${terminal ? 'OPERATION_COMPLETE' : 'MORE_WORK_REMAINS'}',
      p_actor_user_id := '${ACTOR_ID}'::uuid
    )::text
  `);
  phaseCalls.push({ rpc: 'banking_pay_operation_release_lease', phase: operation.phase, elapsed_ms: result.elapsedMs });
  assert.equal(result.value.ok, true, JSON.stringify(result.value));
  return result.value;
}

function readUnselectedCandidateAuthority(target, batchId, selectedCandidateToken) {
  return queryJson(target, `
    WITH candidate_authority AS (
      SELECT candidate_row.id,
             pg_catalog.jsonb_build_object(
               'pay_batch_candidate_id', candidate_row.id,
               'candidate_id', candidate_row.candidate_id,
               'net_bank_amount_pence', pg_catalog.round(COALESCE(candidate_row.net_bank_amount, 0) * 100)::bigint,
               'settlement_status', candidate_row.settlement_status,
               'item_state', COALESCE((
                 SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                   item_row.id, item_row.item_type, COALESCE(item_row.is_voided, false),
                   pg_catalog.round(COALESCE(item_row.amount_ex_vat, 0) * 100)::bigint,
                   pg_catalog.round(COALESCE(item_row.amount_vat, 0) * 100)::bigint,
                   pg_catalog.round(COALESCE(item_row.amount_inc_vat, 0) * 100)::bigint,
                   item_row.reservation_id, item_row.finance_component_id,
                   item_row.pay_bank_transfer_id, item_row.operation_source_key
                 ) ORDER BY item_row.id)
                 FROM public.pay_batch_items AS item_row
                 WHERE item_row.pay_batch_candidate_id = candidate_row.id
               ), '[]'::jsonb),
               'reservation_state', COALESCE((
                 SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                   reservation_row.id, reservation_row.pay_batch_item_id,
                   reservation_row.status, reservation_row.committed_at_utc,
                   reservation_row.settled_at_utc, reservation_row.released_at_utc
                 ) ORDER BY reservation_row.id)
                 FROM public.pay_advance_reservations AS reservation_row
                 JOIN public.pay_batch_items AS reservation_item
                   ON reservation_item.id = reservation_row.pay_batch_item_id
                 WHERE reservation_item.pay_batch_candidate_id = candidate_row.id
                   AND COALESCE(reservation_item.is_voided, false) IS NOT TRUE
               ), '[]'::jsonb),
               'transfer_state', COALESCE((
                 SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                   transfer_row.id, transfer_row.status, transfer_row.rail_state,
                   transfer_row.request_id, transfer_row.rail_tx_id,
                   transfer_row.transfer_group_key
                 ) ORDER BY transfer_row.id)
                 FROM public.pay_bank_transfers AS transfer_row
                 WHERE transfer_row.id IN (
                   SELECT transfer_item.pay_bank_transfer_id
                   FROM public.pay_batch_items AS transfer_item
                   WHERE transfer_item.pay_batch_candidate_id = candidate_row.id
                     AND transfer_item.pay_bank_transfer_id IS NOT NULL
                     AND COALESCE(transfer_item.is_voided, false) IS NOT TRUE
                 )
               ), '[]'::jsonb)
             ) AS authority_json
      FROM public.pay_batch_candidates AS candidate_row
      WHERE candidate_row.pay_batch_id = '${batchId}'::uuid
        AND candidate_row.id <> '${selectedCandidateToken}'::uuid
    )
    SELECT pg_catalog.jsonb_build_object(
      'candidate_count', pg_catalog.count(*)::integer,
      'authority_digest_sha256', private.pay_payment_correction_sha256_v1(
        COALESCE(pg_catalog.jsonb_agg(authority_json ORDER BY id), '[]'::jsonb)
      )
    )::text
    FROM candidate_authority
  `).value;
}

function readCandidateCancellationFinancials(target, candidateToken) {
  return queryJson(target, `
    WITH item_scope AS (
      SELECT item_row.*
      FROM public.pay_batch_items AS item_row
      WHERE item_row.pay_batch_candidate_id = '${candidateToken}'::uuid
    ), reservation_scope AS (
      SELECT reservation_row.*
      FROM public.pay_advance_reservations AS reservation_row
      WHERE reservation_row.pay_batch_candidate_id = '${candidateToken}'::uuid
    )
    SELECT pg_catalog.jsonb_build_object(
      'active_item_count', (SELECT pg_catalog.count(*)::integer FROM item_scope WHERE COALESCE(is_voided, false) IS FALSE),
      'voided_item_count', (SELECT pg_catalog.count(*)::integer FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'active_ex_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_ex_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS FALSE),
      'voided_ex_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_ex_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'active_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS FALSE),
      'voided_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'active_inc_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_inc_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS FALSE),
      'voided_inc_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_inc_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'reservation_count', (SELECT pg_catalog.count(*)::integer FROM reservation_scope),
      'unreleased_reservation_count', (SELECT pg_catalog.count(*)::integer FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_reservation_count', (SELECT pg_catalog.count(*)::integer FROM reservation_scope WHERE released_at_utc IS NOT NULL),
      'unreleased_reserved_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_reserved_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NOT NULL),
      'unreleased_source_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_source_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_source_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_source_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NOT NULL),
      'unreleased_target_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(frozen_rounded_target_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_target_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(frozen_rounded_target_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NOT NULL)
    )::text
  `).value;
}

function runCancellation(target, channel, batchId) {
  const workerId = `h2-cancel-${target.database}-${channel.toLowerCase()}`;
  const phaseCalls = [];
  const cancellationScope = String(process.env.H2_CANCEL_SCOPE || 'WHOLE_BATCH').trim().toUpperCase();
  const paymentState = String(process.env.H2_CANCEL_PAYMENT_STATE || 'DRAFT').trim().toUpperCase();
  const terminalNoMoney = paymentState.endsWith('_FAILED_NO_MONEY');
  const simulateResponseLoss = String(
    process.env.H2_CANCEL_SIMULATE_RESPONSE_LOSS || ''
  ).trim().toLowerCase() === 'true';
  const requestedAction = terminalNoMoney
    ? 'NO_MONEY_RELEASE'
    : (paymentState === 'SCHEDULED_LOCAL_NOT_SENT' ? 'CANCEL_PAYMENT' : 'DRAFT_CANCEL');
  const correctionReason = requestedAction === 'DRAFT_CANCEL'
    ? 'DRAFT_PAYMENT_CANCELLED_BY_USER'
    : (requestedAction === 'NO_MONEY_RELEASE'
      ? 'FAILED_PAYMENT_CONFIRMED_NO_MONEY_RELEASED_BY_USER'
      : 'WHOLE_CANDIDATE_SCHEDULED_PAYMENT_CANCELLED_BEFORE_PROVIDER_SUBMISSION');
  assert.ok(['WHOLE_BATCH', 'ONE_CANDIDATE'].includes(cancellationScope), `unknown H2_CANCEL_SCOPE ${cancellationScope}`);
  assert.ok([
    'DRAFT',
    'SCHEDULED_LOCAL_NOT_SENT',
    'IMMEDIATE_FAILED_NO_MONEY',
    'SCHEDULED_FAILED_NO_MONEY'
  ].includes(paymentState), `unknown H2_CANCEL_PAYMENT_STATE ${paymentState}`);
  if (paymentState !== 'DRAFT') assert.equal(cancellationScope, 'ONE_CANDIDATE');
  let productionShapeActiveJobIsolation = null;
  if (String(process.env.H2_CANCEL_PRODUCTION_SHAPED_SOURCE || '').trim().toLowerCase() === 'true') {
    establishProductionShapedWorkbenchSource(target, batchId);
    // Shaping the old scale-only oracle deliberately fires the real dirty
    // triggers. Those setup jobs are not cancellation work and must not be
    // allowed to advance the source generation after the certified current
    // baseline below has been sealed.
    productionShapeActiveJobIsolation = clearPreexistingCancellationWorkbenchJobs(target);
  }
  const currentnessBefore = String(process.env.H2_CANCEL_ESTABLISH_CURRENT_SOURCE || '').trim().toLowerCase() === 'true'
    ? establishCurrentWorkbenchAuthority(target, batchId)
    : null;
  // The synthetic certification writes exercise the real source-line dirty
  // triggers. Once terminal-current has been proved, discard only those
  // setup-created active jobs so the later drain is attributable solely to
  // the cancellation under test.
  const certifiedBaselineActiveJobIsolation = currentnessBefore
    ? clearPreexistingCancellationWorkbenchJobs(target)
    : null;
  // A real scheduled execution seals its unsent-overlay proof against the
  // already-current Workbench authority inherited from the frozen Draft.
  // Prepare the scheduled payment only after that authority is current, and
  // before the cancellation request is created.
  const scheduledPreparation = paymentState === 'SCHEDULED_LOCAL_NOT_SENT'
    ? establishLocalNotSent(target, batchId, 'SCHEDULED')
    : (terminalNoMoney
      ? establishProviderFailedNoMoney(
        target,
        batchId,
        paymentState === 'IMMEDIATE_FAILED_NO_MONEY' ? 'IMMEDIATE' : 'SCHEDULED'
      )
      : null);
  const before = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'batch_status', batch_row.status,
      'pay_channel', batch_row.batch_kind_fixed,
      'source_workbench_session_id', batch_row.source_workbench_session_id,
      'batch_candidate_count', pg_catalog.count(DISTINCT candidate_row.id)::integer,
      'active_item_count', pg_catalog.count(item_row.id) FILTER (WHERE COALESCE(item_row.is_voided, false) IS FALSE)::integer,
      'active_ex_vat_pence', pg_catalog.round(COALESCE(pg_catalog.sum(item_row.amount_ex_vat) FILTER (WHERE COALESCE(item_row.is_voided, false) IS FALSE), 0) * 100)::bigint,
      'active_vat_pence', pg_catalog.round(COALESCE(pg_catalog.sum(item_row.amount_vat) FILTER (WHERE COALESCE(item_row.is_voided, false) IS FALSE), 0) * 100)::bigint,
      'active_inc_vat_pence', pg_catalog.round(COALESCE(pg_catalog.sum(item_row.amount_inc_vat) FILTER (WHERE COALESCE(item_row.is_voided, false) IS FALSE), 0) * 100)::bigint,
      'reservation_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id),
      'unreleased_reservation_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NULL),
      'released_reservation_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NOT NULL),
      'unreleased_reserved_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reservation_row.reserved_amount), 0) * 100)::bigint FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NULL),
      'released_reserved_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reservation_row.reserved_amount), 0) * 100)::bigint FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NOT NULL),
      'unreleased_source_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reservation_row.reserved_source_amount), 0) * 100)::bigint FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NULL),
      'released_source_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reservation_row.reserved_source_amount), 0) * 100)::bigint FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NOT NULL),
      'unreleased_target_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reservation_row.frozen_rounded_target_amount), 0) * 100)::bigint FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NULL),
      'released_target_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reservation_row.frozen_rounded_target_amount), 0) * 100)::bigint FROM public.pay_advance_reservations AS reservation_row WHERE reservation_row.pay_batch_id = batch_row.id AND reservation_row.released_at_utc IS NOT NULL),
      'provider_attempt_count', (SELECT pg_catalog.count(*)::integer FROM public.banking_pay_operation_provider_attempts),
      'provider_event_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfer_events)
    )::text
    FROM public.pay_batches AS batch_row
    JOIN public.pay_batch_candidates AS candidate_row ON candidate_row.pay_batch_id = batch_row.id
    JOIN public.pay_batch_items AS item_row ON item_row.pay_batch_candidate_id = candidate_row.id
    WHERE batch_row.id = '${batchId}'::uuid
    GROUP BY batch_row.id
  `).value;
  assert.equal(before.batch_status, paymentState === 'DRAFT' ? 'DRAFT' : 'SCHEDULED');
  assert.equal(before.pay_channel, channel);

  let selectedCandidate = null;
  let selectionSnapshotToken = null;
  let selectedCandidateFinancialsBefore = null;
  let unselectedCandidateAuthorityBefore = null;
  let started;
  if (cancellationScope === 'ONE_CANDIDATE') {
    const paymentStatusAction = terminalNoMoney ? 'RELEASE_FAILED_PAYMENT' : requestedAction;
    const pageFilter = terminalNoMoney
      ? `pg_catalog.jsonb_build_object('action','RELEASE_FAILED_PAYMENT','actionable_only',true)`
      : `'{}'::jsonb`;
    const page = queryJson(target, `
      SELECT public.pay_batch_payment_status_page_v1(
        '${batchId}'::uuid,
        '${ACTOR_ID}'::uuid,
        ${pageFilter},
        'STATUS',
        'ASC',
        25,
        NULL::jsonb
      )::text
    `).value;
    assert.equal(page.ok, true, JSON.stringify(page));
    assert.ok(Array.isArray(page.rows) && page.rows.length >= 2, JSON.stringify(page));
    selectionSnapshotToken = page.explicit_snapshot_token;
    assert.ok(selectionSnapshotToken, JSON.stringify(page));
    selectedCandidate = page.rows[0];
    assert.match(selectedCandidate.candidate_token, /^[0-9a-f-]{36}$/i);
    assert.equal(selectedCandidate.active_item_count > 0, true);
    assert.ok(selectedCandidate.available_actions.includes(paymentStatusAction), JSON.stringify(selectedCandidate));
    selectedCandidateFinancialsBefore = readCandidateCancellationFinancials(
      target,
      selectedCandidate.candidate_token
    );
    assert.equal(selectedCandidateFinancialsBefore.active_item_count, selectedCandidate.active_item_count);
    unselectedCandidateAuthorityBefore = readUnselectedCandidateAuthority(
      target,
      batchId,
      selectedCandidate.candidate_token
    );
    assert.equal(unselectedCandidateAuthorityBefore.candidate_count, before.batch_candidate_count - 1);
    started = queryJson(target, `
      SELECT public.pay_payment_correction_request_start(
        p_pay_batch_id := '${batchId}'::uuid,
        p_selection_json := pg_catalog.jsonb_build_object(
          'command','PREPARE',
          'context','CURRENT_PAYMENT_STATUS',
          'contract_version',1,
          'mode','EXPLICIT',
          'requested_action','${requestedAction}',
          'snapshot_token','${selectionSnapshotToken}',
          'sort_key','STATUS',
          'sort_direction','ASC',
          'explicit_candidate_tokens',pg_catalog.jsonb_build_array('${selectedCandidate.candidate_token}'),
          'idempotency_key','h2-v1-v8-one-candidate-${paymentState.toLowerCase()}-${target.database}-${channel.toLowerCase()}'
        ),
        p_reason := '${correctionReason}',
        p_actor_user_id := '${ACTOR_ID}'::uuid,
        p_source_bank_event_id := NULL::uuid,
        p_auto_requested := false,
        p_accepted_resolution_json := NULL::jsonb
      )::text
    `);
  } else {
    started = queryJson(target, `
      SELECT public.pay_batch_cancel(
        p_pay_batch_id := '${batchId}'::uuid,
        p_actor_user_id := '${ACTOR_ID}'::uuid,
        p_reason := 'DRAFT_PAYMENT_CANCELLED_BY_USER',
        p_correction_request_id := NULL::uuid,
        p_work_item_id := NULL::uuid
      )::text
    `);
  }
  phaseCalls.push({
    rpc: cancellationScope === 'ONE_CANDIDATE'
      ? 'pay_payment_correction_request_start'
      : 'pay_batch_cancel',
    phase: 'PREPARE_SELECTION',
    elapsed_ms: started.elapsedMs
  });
  assert.equal(started.value.ok, true, JSON.stringify(started.value));
  assert.equal(started.value.phase, 'PREPARE_SELECTION', JSON.stringify(started.value));
  const requestId = started.value.correction_request_id;
  const operationId = started.value.operation_id;
  assert.match(requestId, /^[0-9a-f-]{36}$/i);
  assert.match(operationId, /^[0-9a-f-]{36}$/i);

  let prepareResponseLossReplay = null;
  if (simulateResponseLoss && cancellationScope === 'ONE_CANDIDATE') {
    const replayedStart = queryJson(target, `
      SELECT public.pay_payment_correction_request_start(
        p_pay_batch_id := '${batchId}'::uuid,
        p_selection_json := pg_catalog.jsonb_build_object(
          'command','PREPARE',
          'context','CURRENT_PAYMENT_STATUS',
          'contract_version',1,
          'mode','EXPLICIT',
          'requested_action','${requestedAction}',
          'snapshot_token','${selectionSnapshotToken}',
          'sort_key','STATUS',
          'sort_direction','ASC',
          'explicit_candidate_tokens',pg_catalog.jsonb_build_array('${selectedCandidate.candidate_token}'),
          'idempotency_key','h2-v1-v8-one-candidate-${paymentState.toLowerCase()}-${target.database}-${channel.toLowerCase()}'
        ),
        p_reason := '${correctionReason}',
        p_actor_user_id := '${ACTOR_ID}'::uuid,
        p_source_bank_event_id := NULL::uuid,
        p_auto_requested := false,
        p_accepted_resolution_json := NULL::jsonb
      )::text
    `);
    phaseCalls.push({
      rpc: 'pay_payment_correction_request_start',
      phase: 'PREPARE_SELECTION_RESPONSE_LOSS_REPLAY',
      elapsed_ms: replayedStart.elapsedMs
    });
    assert.equal(replayedStart.value.ok, true, JSON.stringify(replayedStart.value));
    assert.equal(replayedStart.value.existing_request, true, JSON.stringify(replayedStart.value));
    assert.equal(replayedStart.value.correction_request_id, requestId);
    assert.equal(replayedStart.value.operation_id, operationId);
    prepareResponseLossReplay = {
      same_request: true,
      same_operation: true,
      existing_request: true
    };
  }

  const prepareClaim = claim(target, operationId, workerId);
  phaseCalls.push({ rpc: 'banking_pay_operation_claim_next', phase: 'PREPARE_SELECTION', elapsed_ms: prepareClaim.elapsedMs });
  assert.equal(prepareClaim.value.claimed, true, JSON.stringify(prepareClaim.value));
  const prepared = queryJson(target, `
    SELECT public.pay_payment_correction_process_chunk(
      '${requestId}'::uuid, 50, '${workerId}', '${ACTOR_ID}'::uuid
    )::text
  `);
  phaseCalls.push({ rpc: 'pay_payment_correction_process_chunk', phase: 'PREPARE_SELECTION', elapsed_ms: prepared.elapsedMs });
  assert.equal(prepared.value.ok, true, JSON.stringify(prepared.value));
  assert.equal(prepared.value.phase, 'AWAITING_REAUTHENTICATION', JSON.stringify(prepared.value));
  assert.equal(prepared.value.complete, true, JSON.stringify(prepared.value));

  const request = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'status', status,
      'selection_hash', selection_hash,
      'plan_hash', plan_hash
    )::text
    FROM public.pay_payment_correction_requests
    WHERE id = '${requestId}'::uuid
  `).value;
  assert.equal(request.status, 'PLANNED');
  assert.match(request.selection_hash, /^[0-9a-f]{64}$/);
  assert.match(request.plan_hash, /^[0-9a-f]{64}$/);

  const proofHash = crypto.createHash('sha256').update(`proof:${target.database}:${channel}`).digest('hex');
  const sessionHash = crypto.createHash('sha256').update(`session:${target.database}:${channel}`).digest('hex');
  const bound = queryJson(target, `
    SELECT public.pay_payment_correction_reauth_bind_v1(
      '${requestId}'::uuid,
      '${ACTOR_ID}'::uuid,
      '${sessionHash}',
      '${proofHash}',
      pg_catalog.date_trunc('second', pg_catalog.clock_timestamp()),
      pg_catalog.date_trunc('second', pg_catalog.clock_timestamp()) + interval '5 minutes'
    )::text
  `);
  phaseCalls.push({ rpc: 'pay_payment_correction_reauth_bind_v1', phase: 'AWAITING_REAUTHENTICATION', elapsed_ms: bound.elapsedMs });
  assert.equal(bound.value.ok, true, JSON.stringify(bound.value));

  const authorised = queryJson(target, `
    SELECT public.pay_payment_correction_request_start(
      p_pay_batch_id := '${batchId}'::uuid,
      p_selection_json := pg_catalog.jsonb_build_object(
        'command', 'START_PREPARED',
        'context', 'START_PREPARED',
        'correction_request_id', '${requestId}'::uuid,
        'proof_hash', '${proofHash}',
        'selection_hash', '${request.selection_hash}',
        'plan_hash', '${request.plan_hash}'
      ),
      p_reason := '${correctionReason}',
      p_actor_user_id := '${ACTOR_ID}'::uuid,
      p_source_bank_event_id := NULL::uuid,
      p_auto_requested := false,
      p_accepted_resolution_json := NULL::jsonb
    )::text
  `);
  phaseCalls.push({ rpc: 'pay_payment_correction_request_start', phase: 'START_PREPARED', elapsed_ms: authorised.elapsedMs });
  assert.equal(authorised.value.ok, true, JSON.stringify(authorised.value));
  assert.equal(authorised.value.phase, 'EXPAND_WORK', JSON.stringify(authorised.value));

  const qBoundCancellationAuthority = paymentState === 'SCHEDULED_LOCAL_NOT_SENT'
    ? queryJson(target, `
        SELECT pg_catalog.jsonb_build_object(
          'contract_version', input_json#>>'{cancellation_reversion_q_bound_pre_request_start_v1,contract_version}',
          'execution_operation_resolution', input_json#>>'{cancellation_reversion_q_bound_pre_request_start_v1,execution_operation_resolution}',
          'all_admitted', COALESCE((input_json#>>'{cancellation_reversion_q_bound_pre_request_start_v1,all_admitted}')::boolean, false),
          'candidate_authority_count', (
            SELECT pg_catalog.count(*)::integer
            FROM pg_catalog.jsonb_object_keys(COALESCE(
              input_json#>'{cancellation_reversion_q_bound_pre_request_start_v1,candidate_authorities}',
              '{}'::jsonb
            )) AS authority_key(key)
          )
        )::text
        FROM public.banking_pay_operations
        WHERE id = '${operationId}'::uuid
      `).value
    : null;
  if (qBoundCancellationAuthority) {
    assert.equal(qBoundCancellationAuthority.contract_version, 'CANCELLATION_REVERSION_Q_BOUND_PRE_REQUEST_START_SET_V1', JSON.stringify(qBoundCancellationAuthority));
    if (qBoundCancellationAuthority.execution_operation_resolution === 'EXACT') {
      assert.equal(qBoundCancellationAuthority.all_admitted, true, JSON.stringify(qBoundCancellationAuthority));
      assert.equal(qBoundCancellationAuthority.candidate_authority_count, 1, JSON.stringify(qBoundCancellationAuthority));
    } else {
      // The current chain owner deliberately records ordinary scheduled rows
      // without a finance/reservation schedule transition as diagnostic-only.
      // The cancellation must therefore take the established safe fallback;
      // it must never be mislabelled as certified fast reversion.
      assert.equal(qBoundCancellationAuthority.execution_operation_resolution, 'EXECUTION_RESIDUAL_EXECUTION_OPERATION_MISSING', JSON.stringify(qBoundCancellationAuthority));
      assert.equal(qBoundCancellationAuthority.all_admitted, false, JSON.stringify(qBoundCancellationAuthority));
      assert.equal(qBoundCancellationAuthority.candidate_authority_count, 0, JSON.stringify(qBoundCancellationAuthority));
      assert.ok(scheduledPreparation?.execution_overlay_chain_candidate_count > 0, JSON.stringify(scheduledPreparation));
      assert.equal(scheduledPreparation.execution_overlay_chain_closed_candidate_count, 0, JSON.stringify(scheduledPreparation));
    }
  }

  let operation = readOperation(target, operationId).value;
  let workbenchDrain = null;
  let postCommitCurrentnessRetryCount = 0;
  const postCommitCurrentnessRetryEvidence = [];
  let processResponseLossReplay = null;
  let iteration = 0;
  while (operation.status !== 'COMPLETE' && operation.phase !== 'COMPLETE') {
    iteration += 1;
    assert.ok(iteration <= 40, `cancellation did not terminate: ${JSON.stringify(operation)}`);

    // A Worker delivery runs in a fresh transaction. The explicit operation-id
    // claim below is therefore due as soon as the prior zero-delay release has
    // committed; no sleep or timeout relaxation is used.
    const claimed = claim(target, operationId, workerId);
    phaseCalls.push({ rpc: 'banking_pay_operation_claim_next', phase: operation.phase, elapsed_ms: claimed.elapsedMs });
    assert.equal(claimed.value.claimed, true, JSON.stringify(claimed.value));

    let advanced;
    try {
      advanced = queryJson(target, `
        SELECT public.pay_payment_correction_process_chunk(
          '${requestId}'::uuid, 50, '${workerId}', '${ACTOR_ID}'::uuid
        )::text
      `);
    } catch (error) {
      const currentnessRetryCode = [
        'PAYMENT_CANCEL_CURRENT_OR_REPAIR_OWNER_REQUIRED',
        'PAYMENT_CORRECTION_WORKBENCH_ROUTE_NOT_PHYSICALLY_CURRENT',
        'PAYMENT_CORRECTION_POST_COMMIT_AUTHORITY_NOT_FINALIZED',
        'PAYMENT_CORRECTION_WORKBENCH_REVERSION_RETRY',
        'PAYMENT_CORRECTION_WORKBENCH_OVERLAY_RESTORE_RETRY'
      ].find((code) => hasDiagnosticToken(error, code)) || null;
      const exactPostCommitCurrentnessRetry = operation.phase === 'REFRESH_WORKBENCH'
        && currentnessRetryCode != null;
      if (exactPostCommitCurrentnessRetry) {
        postCommitCurrentnessRetryCount += 1;
        const authorityEvidence = readPostCommitAuthorityDiagnostics(target, requestId, operationId);
        postCommitCurrentnessRetryEvidence.push({
          retry_ordinal: postCommitCurrentnessRetryCount,
          retry_code: currentnessRetryCode,
          candidate_count: authorityEvidence.candidate_count,
          finalized_count: authorityEvidence.finalized_count,
          all_finalized: authorityEvidence.all_finalized,
          rejection_reasons: Object.values(authorityEvidence.candidate_authorities || {})
            .map((value) => value.rejection_reason)
            .filter(Boolean)
        });
        assert.ok(postCommitCurrentnessRetryCount <= 1, JSON.stringify({
          code: 'POST_COMMIT_CURRENTNESS_RETRY_DID_NOT_CONVERGE',
          evidence: postCommitCurrentnessRetryEvidence
        }));
        operation = readOperation(target, operationId).value;
        release(target, operationId, workerId, operation, phaseCalls);
        assert.equal(
          String(process.env.H2_CANCEL_DRAIN_WORKBENCH || '').trim().toLowerCase(),
          'true',
          'exact post-commit currentness retry requires the rollback-only Workbench runner in this fixture'
        );
        workbenchDrain = drainCancellationWorkbenchSourceBuilds(target, batchId);
        operation = readOperation(target, operationId).value;
        continue;
      }
      throw Object.assign(error, {
        code: 'PAYMENT_CORRECTION_PROCESS_CHUNK_SQL_FAILURE',
        phase: operation.phase,
        phaseCalls,
        requestId,
        operationId
      });
    }
    phaseCalls.push({ rpc: 'pay_payment_correction_process_chunk', phase: operation.phase, elapsed_ms: advanced.elapsedMs });
    if (advanced.value.ok !== true) {
      throw Object.assign(new Error(JSON.stringify(advanced.value)), {
        code: advanced.value.code || 'PAYMENT_CORRECTION_ADVANCE_FAILED',
        phase: operation.phase,
        payload: advanced.value,
        phaseCalls,
        requestId,
        operationId
      });
    }
    if (advanced.value.requires_user_action === true) {
      throw Object.assign(new Error(JSON.stringify(advanced.value)), {
        code: advanced.value.code || advanced.value.result?.code || 'PAYMENT_CORRECTION_USER_ACTION_REQUIRED',
        phase: operation.phase,
        payload: advanced.value,
        phaseCalls,
        requestId,
        operationId
      });
    }

    if (simulateResponseLoss
        && cancellationScope === 'ONE_CANDIDATE'
        && operation.phase === 'PROCESS_CHUNKS'
        && processResponseLossReplay == null) {
      const effectAfterCommittedResponse = readCandidateCancellationFinancials(
        target,
        selectedCandidate.candidate_token
      );
      // Discard the first successful response and invoke the same resumable
      // owner again. It must observe the persisted next phase and must not
      // repeat any item, reservation or amount mutation.
      const replayedProcess = queryJson(target, `
        SELECT public.pay_payment_correction_process_chunk(
          '${requestId}'::uuid, 50, '${workerId}', '${ACTOR_ID}'::uuid
        )::text
      `);
      phaseCalls.push({
        rpc: 'pay_payment_correction_process_chunk',
        phase: 'PROCESS_CHUNKS_RESPONSE_LOSS_REPLAY',
        elapsed_ms: replayedProcess.elapsedMs
      });
      assert.equal(replayedProcess.value.ok, true, JSON.stringify(replayedProcess.value));
      const effectAfterReplay = readCandidateCancellationFinancials(
        target,
        selectedCandidate.candidate_token
      );
      assert.deepEqual(effectAfterReplay, effectAfterCommittedResponse);
      processResponseLossReplay = {
        persisted_phase_after_first_response: advanced.value.phase,
        resumed_phase: replayedProcess.value.phase,
        candidate_financial_effect_repeated: false,
        exact_candidate_financials_preserved: true
      };
    }

    operation = readOperation(target, operationId).value;
    release(target, operationId, workerId, operation, phaseCalls);
    operation = readOperation(target, operationId).value;
  }

  if (!workbenchDrain && String(process.env.H2_CANCEL_DRAIN_WORKBENCH || '').trim().toLowerCase() === 'true') {
    workbenchDrain = drainCancellationWorkbenchSourceBuilds(target, batchId);
  }
  if (workbenchDrain && productionShapeActiveJobIsolation) {
    workbenchDrain.production_shape_active_job_isolation = productionShapeActiveJobIsolation;
  }
  if (workbenchDrain && certifiedBaselineActiveJobIsolation) {
    workbenchDrain.certified_baseline_active_job_isolation = certifiedBaselineActiveJobIsolation;
  }

  const after = queryJson(target, `
    WITH batch_scope AS (
      SELECT batch_row.*
      FROM public.pay_batches AS batch_row
      WHERE batch_row.id = '${batchId}'::uuid
    ), candidate_scope AS (
      SELECT candidate_row.*
      FROM public.pay_batch_candidates AS candidate_row
      WHERE candidate_row.pay_batch_id = '${batchId}'::uuid
    ), item_scope AS (
      SELECT item_row.*
      FROM public.pay_batch_items AS item_row
      JOIN candidate_scope AS candidate_row ON candidate_row.id = item_row.pay_batch_candidate_id
    ), reservation_scope AS (
      SELECT reservation_row.*
      FROM public.pay_advance_reservations AS reservation_row
      WHERE reservation_row.pay_batch_id = '${batchId}'::uuid
    ), workbench_scope AS (
      SELECT preview_row.*
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      JOIN batch_scope AS batch_row ON batch_row.source_workbench_session_id = preview_row.session_id
      WHERE preview_row.candidate_id IN (SELECT candidate_id FROM candidate_scope)
    )
    SELECT pg_catalog.jsonb_build_object(
      'pay_channel', (SELECT batch_kind_fixed FROM batch_scope),
      'batch_status', (SELECT status FROM batch_scope),
      'source_workbench_session_id', (SELECT source_workbench_session_id FROM batch_scope),
      'batch_candidate_count', (SELECT pg_catalog.count(*)::integer FROM candidate_scope),
      'active_item_count', (SELECT pg_catalog.count(*)::integer FROM item_scope WHERE COALESCE(is_voided, false) IS FALSE),
      'voided_item_count', (SELECT pg_catalog.count(*)::integer FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'voided_ex_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_ex_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'voided_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'voided_inc_vat_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_inc_vat), 0) * 100)::bigint FROM item_scope WHERE COALESCE(is_voided, false) IS TRUE),
      'reservation_count', (SELECT pg_catalog.count(*)::integer FROM reservation_scope),
      'unreleased_reservation_count', (SELECT pg_catalog.count(*)::integer FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_reservation_count', (SELECT pg_catalog.count(*)::integer FROM reservation_scope WHERE released_at_utc IS NOT NULL),
      'unreleased_reserved_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_reserved_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NOT NULL),
      'unreleased_source_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_source_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_source_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(reserved_source_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NOT NULL),
      'unreleased_target_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(frozen_rounded_target_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NULL),
      'released_target_amount_pence', (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(frozen_rounded_target_amount), 0) * 100)::bigint FROM reservation_scope WHERE released_at_utc IS NOT NULL),
      'request_status', (SELECT status FROM public.pay_payment_correction_requests WHERE id = '${requestId}'::uuid),
      'operation_status', (SELECT status FROM public.banking_pay_operations WHERE id = '${operationId}'::uuid),
      'operation_phase', (SELECT phase FROM public.banking_pay_operations WHERE id = '${operationId}'::uuid),
      'request_candidate_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_payment_correction_request_candidates WHERE correction_request_id = '${requestId}'::uuid),
      'applied_work_item_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_payment_correction_work_items WHERE correction_request_id = '${requestId}'::uuid AND status = 'APPLIED'),
      'applied_correction_item_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_payment_correction_items WHERE correction_request_id = '${requestId}'::uuid AND status = 'APPLIED'),
      'workbench_row_count', (SELECT pg_catalog.count(*)::integer FROM workbench_scope),
      'workbench_ready_count', (SELECT pg_catalog.count(*)::integer FROM workbench_scope WHERE status = 'READY'),
      'workbench_superseded_count', (SELECT pg_catalog.count(*)::integer FROM workbench_scope WHERE status = 'SUPERSEDED'),
      'workbench_cancel_return_history_count', (
        SELECT pg_catalog.count(*)::integer
        FROM workbench_scope
        WHERE status = 'SUPERSEDED'
          AND selected IS FALSE
          AND selection_state = 'SUPERSEDED'
          AND COALESCE((row_json->>'post_cancel_selection_restored')::boolean, false) IS TRUE
      ),
      'workbench_current_source_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        WHERE source_row.session_id = (SELECT source_workbench_session_id FROM batch_scope)
          AND source_row.candidate_id IN (SELECT candidate_id FROM candidate_scope)
          AND source_row.status = 'CURRENT'
      ),
      'workbench_selected_count', (SELECT pg_catalog.count(*)::integer FROM workbench_scope WHERE selected IS TRUE AND selection_state = 'SELECTED'),
      'workbench_not_selectable_count', (SELECT pg_catalog.count(*)::integer FROM workbench_scope WHERE selected IS FALSE AND selection_state = 'NOT_SELECTABLE'),
      'provider_attempt_count', (SELECT pg_catalog.count(*)::integer FROM public.banking_pay_operation_provider_attempts),
      'provider_event_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfer_events),
      'cancellation_audit_count', (SELECT pg_catalog.count(*)::integer FROM public.audit_events WHERE object_id_text = '${batchId}' AND action = 'PAY_BATCH_CANCELLED'),
      'correction_work_item_audit_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.audit_events AS audit_row
        JOIN public.pay_payment_correction_work_items AS work_item_row
          ON work_item_row.id::text = audit_row.object_id_text
        WHERE work_item_row.correction_request_id = '${requestId}'::uuid
          AND audit_row.action = 'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT'
      ),
      'correction_work_item_result_evidence_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS work_item_row
        WHERE work_item_row.correction_request_id = '${requestId}'::uuid
          AND work_item_row.status = 'APPLIED'
          AND work_item_row.result_json->>'ok' = 'true'
          AND work_item_row.result_json->>'status' = 'APPLIED'
          AND work_item_row.result_json->>'correction_item_kind' = 'NO_MONEY_UNWIND'
          AND work_item_row.result_json->>'work_item_id' = work_item_row.id::text
          AND work_item_row.result_json->>'correction_request_id' = '${requestId}'::text
          AND work_item_row.result_json->>'pay_batch_id' = '${batchId}'::text
          AND work_item_row.result_json->>'applied_at_utc' IS NOT NULL
      ),
      'queued_or_running_workbench_job_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.banking_pay_workbench_jobs AS job_row
        WHERE job_row.session_id = (SELECT source_workbench_session_id FROM batch_scope)
          AND job_row.candidate_id IN (SELECT candidate_id FROM candidate_scope)
          AND job_row.status IN ('QUEUED', 'RUNNING')
      ),
      'latest_refresh_status', (
        SELECT chunk_row.result_json->'workbench_refresh_nudge'->>'refresh_status'
        FROM public.banking_pay_operation_chunks AS chunk_row
        WHERE chunk_row.operation_id = '${operationId}'::uuid
          AND chunk_row.phase = 'REFRESH_WORKBENCH'
          AND chunk_row.chunk_type = 'CANDIDATE_SCOPE'
          AND chunk_row.status = 'COMPLETE'
        ORDER BY chunk_row.sequence_no DESC
        LIMIT 1
      ),
      'source_session_preserved', (SELECT source_workbench_session_id FROM batch_scope) = '${before.source_workbench_session_id}'::uuid
    )::text
  `).value;

  if (cancellationScope === 'ONE_CANDIDATE') {
    const selectedCandidateFinancialsAfter = readCandidateCancellationFinancials(
      target,
      selectedCandidate.candidate_token
    );
    const unselectedCandidateAuthorityAfter = readUnselectedCandidateAuthority(
      target,
      batchId,
      selectedCandidate.candidate_token
    );
    // Cancelling one whole Candidate invalidates the prior authorisation or
    // schedule for the remaining Candidate scope. The established owner
    // deliberately returns that intact remainder to DRAFT for recalculation.
    assert.equal(after.batch_status, 'DRAFT');
    assert.equal(after.active_item_count, before.active_item_count - selectedCandidate.active_item_count);
    assert.equal(after.voided_item_count, selectedCandidate.active_item_count);
    assert.equal(selectedCandidateFinancialsAfter.active_item_count, 0);
    assert.equal(
      selectedCandidateFinancialsAfter.voided_item_count - selectedCandidateFinancialsBefore.voided_item_count,
      selectedCandidateFinancialsBefore.active_item_count
    );
    for (const amountKind of ['ex_vat', 'vat', 'inc_vat']) {
      assert.equal(selectedCandidateFinancialsAfter[`active_${amountKind}_pence`], 0);
      assert.equal(
        selectedCandidateFinancialsAfter[`voided_${amountKind}_pence`] - selectedCandidateFinancialsBefore[`voided_${amountKind}_pence`],
        selectedCandidateFinancialsBefore[`active_${amountKind}_pence`]
      );
    }
    assert.equal(selectedCandidateFinancialsAfter.reservation_count, selectedCandidateFinancialsBefore.reservation_count);
    assert.equal(selectedCandidateFinancialsAfter.unreleased_reservation_count, 0);
    assert.equal(
      selectedCandidateFinancialsAfter.released_reservation_count - selectedCandidateFinancialsBefore.released_reservation_count,
      selectedCandidateFinancialsBefore.unreleased_reservation_count
    );
    for (const amountKind of ['reserved', 'source', 'target']) {
      assert.equal(selectedCandidateFinancialsAfter[`unreleased_${amountKind}_amount_pence`], 0);
      assert.equal(
        selectedCandidateFinancialsAfter[`released_${amountKind}_amount_pence`] - selectedCandidateFinancialsBefore[`released_${amountKind}_amount_pence`],
        selectedCandidateFinancialsBefore[`unreleased_${amountKind}_amount_pence`]
      );
    }
    assert.deepEqual(unselectedCandidateAuthorityAfter, unselectedCandidateAuthorityBefore);
  } else {
    assert.equal(after.batch_status, 'CANCELLED');
    assert.equal(after.active_item_count, 0);
    assert.equal(after.voided_item_count, before.active_item_count);
    assert.equal(after.voided_ex_vat_pence, before.active_ex_vat_pence);
    assert.equal(after.voided_vat_pence, before.active_vat_pence);
    assert.equal(after.voided_inc_vat_pence, before.active_inc_vat_pence);
    assert.equal(after.reservation_count, before.reservation_count);
    assert.equal(after.unreleased_reservation_count, 0);
    assert.equal(
      after.released_reservation_count - before.released_reservation_count,
      before.unreleased_reservation_count
    );
    for (const amountKind of ['reserved', 'source', 'target']) {
      assert.equal(after[`unreleased_${amountKind}_amount_pence`], 0);
      assert.equal(
        after[`released_${amountKind}_amount_pence`] - before[`released_${amountKind}_amount_pence`],
        before[`unreleased_${amountKind}_amount_pence`]
      );
    }
  }
  assert.equal(after.batch_candidate_count, before.batch_candidate_count);
  assert.equal(after.request_candidate_count, cancellationScope === 'ONE_CANDIDATE' ? 1 : before.batch_candidate_count);
  assert.equal(after.applied_work_item_count, cancellationScope === 'ONE_CANDIDATE' ? 1 : before.batch_candidate_count);
  assert.ok(after.applied_correction_item_count >= (cancellationScope === 'ONE_CANDIDATE' ? selectedCandidate.active_item_count : before.active_item_count));
  assert.equal(after.request_status, 'APPLIED');
  assert.equal(after.operation_status, 'COMPLETE');
  assert.equal(after.operation_phase, 'COMPLETE');
  assert.equal(after.provider_attempt_count, before.provider_attempt_count);
  assert.equal(after.provider_event_count, before.provider_event_count);
  assert.equal(after.source_session_preserved, true);
  if (cancellationScope === 'WHOLE_BATCH') {
    assert.ok(after.cancellation_audit_count >= 1);
  } else {
    // The established no-money owner writes its durable audit evidence to the
    // correction work item. Its separate _imp_debug_audit call is deliberately
    // controlled by invoice_debug and is not a permanent audit-row contract.
    assert.equal(after.correction_work_item_result_evidence_count, after.applied_work_item_count);
  }
  if (workbenchDrain) {
    assert.equal(after.queued_or_running_workbench_job_count, 0);
    assert.equal(after.workbench_ready_count, after.workbench_current_source_count);
    assert.equal(
      after.workbench_cancel_return_history_count,
      qBoundCancellationAuthority?.all_admitted === true
        ? (cancellationScope === 'ONE_CANDIDATE'
          ? selectedCandidate.active_item_count
          : before.active_item_count)
        : 0
    );
    assert.ok(after.workbench_superseded_count >= after.workbench_cancel_return_history_count);
    assert.equal(
      after.workbench_row_count,
      after.workbench_ready_count + after.workbench_superseded_count
    );
  } else {
    assert.equal(after.queued_or_running_workbench_job_count, cancellationScope === 'ONE_CANDIDATE' ? 1 : before.batch_candidate_count);
    assert.equal(after.latest_refresh_status, 'STAGED');
  }
  assert.ok(phaseCalls.every((call) => call.elapsed_ms < 15000), JSON.stringify(phaseCalls));

  const normalized = {
    cancellation_scope: cancellationScope,
    payment_state_before: paymentState,
    requested_action: requestedAction,
    reversion_route: qBoundCancellationAuthority?.all_admitted === true
      ? 'CERTIFIED_FAST_REVERSION'
      : 'SAFE_SOURCE_REBUILD_FALLBACK',
    q_bound_cancellation_authority: qBoundCancellationAuthority,
    pay_channel: after.pay_channel,
    batch_status: after.batch_status,
    batch_candidate_count: after.batch_candidate_count,
    active_item_count_before: before.active_item_count,
    active_item_count_after: after.active_item_count,
    voided_item_count: after.voided_item_count,
    active_ex_vat_pence_before: before.active_ex_vat_pence,
    voided_ex_vat_pence: after.voided_ex_vat_pence,
    active_vat_pence_before: before.active_vat_pence,
    voided_vat_pence: after.voided_vat_pence,
    active_inc_vat_pence_before: before.active_inc_vat_pence,
    voided_inc_vat_pence: after.voided_inc_vat_pence,
    reservation_count_before: before.reservation_count,
    reservation_count_after: after.reservation_count,
    unreleased_reservation_count_before: before.unreleased_reservation_count,
    unreleased_reservation_count_after: after.unreleased_reservation_count,
    released_reservation_count_before: before.released_reservation_count,
    released_reservation_count_after: after.released_reservation_count,
    unreleased_reserved_amount_pence_before: before.unreleased_reserved_amount_pence,
    unreleased_reserved_amount_pence_after: after.unreleased_reserved_amount_pence,
    released_reserved_amount_pence_before: before.released_reserved_amount_pence,
    released_reserved_amount_pence_after: after.released_reserved_amount_pence,
    unreleased_source_amount_pence_before: before.unreleased_source_amount_pence,
    unreleased_source_amount_pence_after: after.unreleased_source_amount_pence,
    released_source_amount_pence_before: before.released_source_amount_pence,
    released_source_amount_pence_after: after.released_source_amount_pence,
    unreleased_target_amount_pence_before: before.unreleased_target_amount_pence,
    unreleased_target_amount_pence_after: after.unreleased_target_amount_pence,
    released_target_amount_pence_before: before.released_target_amount_pence,
    released_target_amount_pence_after: after.released_target_amount_pence,
    request_status: after.request_status,
    operation_status: after.operation_status,
    operation_phase: after.operation_phase,
    post_commit_currentness_retry_count: postCommitCurrentnessRetryCount,
    post_commit_currentness_retry_evidence: postCommitCurrentnessRetryEvidence,
    prepare_response_loss_replay: prepareResponseLossReplay,
    process_response_loss_replay: processResponseLossReplay,
    request_candidate_count: after.request_candidate_count,
    applied_work_item_count: after.applied_work_item_count,
    applied_correction_item_count: after.applied_correction_item_count,
    workbench_row_count: after.workbench_row_count,
    workbench_ready_count: after.workbench_ready_count,
    workbench_superseded_count: after.workbench_superseded_count,
    workbench_cancel_return_history_count: after.workbench_cancel_return_history_count,
    workbench_current_source_count: after.workbench_current_source_count,
    workbench_selected_count: after.workbench_selected_count,
    workbench_not_selectable_count: after.workbench_not_selectable_count,
    queued_or_running_workbench_job_count: after.queued_or_running_workbench_job_count,
    latest_refresh_status: after.latest_refresh_status,
    provider_attempt_delta: after.provider_attempt_count - before.provider_attempt_count,
    provider_event_delta: after.provider_event_count - before.provider_event_count,
    cancellation_audit_present: cancellationScope === 'WHOLE_BATCH'
      ? after.cancellation_audit_count >= 1
      : after.correction_work_item_result_evidence_count === after.applied_work_item_count,
    optional_debug_audit_count: cancellationScope === 'ONE_CANDIDATE'
      ? after.correction_work_item_audit_count
      : null,
    unselected_candidate_authority_preserved: cancellationScope === 'ONE_CANDIDATE' ? true : null,
    source_session_preserved: after.source_session_preserved,
    policy_x_source: 'FROZEN_DRAFT_ITEMS_AND_CORRECTION_MEMBERSHIP'
  };

  return {
    normalized,
    scheduled_preparation: scheduledPreparation,
    currentness_before: currentnessBefore,
    workbench_drain: workbenchDrain,
    digest_sha256: sha256(normalized),
    operation_iteration_count: iteration,
    phase_call_count: phaseCalls.length,
    maximum_call_ms: Math.max(...phaseCalls.map((call) => call.elapsed_ms)),
    phase_calls: phaseCalls
  };
}

function runTarget(target) {
  cloneDatabase(target);
  try {
    const scaleFinancialNormalization = normalizeCancellationScaleFinancials(target);
    const preexistingWorkbenchIsolation = clearPreexistingCancellationWorkbenchJobs(target);
    // The long-lived disposable source snapshot predates the repository's
    // controlled reconciliation envelope. Install that immutable authority in
    // the clone so a cancellation-return test reaches reconciliation instead
    // of correctly stopping on missing calibration evidence.
    installRepositoryReconciliationEnvelope(target);
    const replacementInstalled = String(process.env.H2_CANCEL_APPLY_REPLACEMENT || '').trim().toLowerCase() === 'true';
    if (replacementInstalled) installReplacement(target);
    const oneCandidateSelectionIntegrityInstalled = String(process.env.H2_CANCEL_APPLY_ONE_CANDIDATE_INTEGRITY || '').trim().toLowerCase() === 'true';
    if (oneCandidateSelectionIntegrityInstalled) installOneCandidateSelectionIntegrity(target);
    const reservationEvidencePrecedenceInstalled = String(process.env.H2_CANCEL_APPLY_RESERVATION_EVIDENCE_PRECEDENCE || '').trim().toLowerCase() === 'true';
    if (reservationEvidencePrecedenceInstalled) installReservationEvidencePrecedence(target);
    const bankEventClassificationConstraintInstalled = String(process.env.H2_CANCEL_APPLY_BANK_EVENT_CLASSIFICATION || '').trim().toLowerCase() === 'true';
    if (bankEventClassificationConstraintInstalled) installBankEventClassificationConstraint(target);
    const noMoneyUnwindResultArityInstalled = String(process.env.H2_CANCEL_APPLY_NO_MONEY_RESULT_ARITY || '').trim().toLowerCase() === 'true';
    if (noMoneyUnwindResultArityInstalled) installNoMoneyUnwindResultArity(target);
    const diagnosticWrapperInstalled = String(process.env.H2_CANCEL_INSTRUMENT_REFRESH || '').trim().toLowerCase() === 'true';
    if (diagnosticWrapperInstalled) installDiagnosticCancelSafeWrapper(target);
    const diagnosticProcessInstalled = String(process.env.H2_CANCEL_INSTRUMENT_PROCESS_ROUTE || '').trim().toLowerCase() === 'true';
    if (diagnosticProcessInstalled) installDiagnosticCorrectionRoute(target);
    const completionAuditCandidateInstalled = String(process.env.H2_CANCEL_APPLY_COMPLETION_AUDIT || '').trim().toLowerCase() === 'true';
    if (completionAuditCandidateInstalled) installCancellationCompletionAuditCandidate(target);
    else {
      const routeHandoffCandidateInstalled = String(process.env.H2_CANCEL_APPLY_ROUTE_HANDOFF || '').trim().toLowerCase() === 'true';
      if (routeHandoffCandidateInstalled) installCorrectionRouteHandoffCandidate(target);
    }
    const setPageEnabled = String(process.env.H2_CANCEL_SET_PAGE || '').trim().toLowerCase() === 'true';
    execSql(target, `
      UPDATE public.settings_defaults
      SET banking_pay_candidate_cancellation_enabled = true,
          banking_pay_draft_overlay_fast_cancel_v1_enabled = false,
          banking_pay_pre_bank_cancel_set_page_v1_enabled = ${setPageEnabled ? 'true' : 'false'},
          banking_pay_cancellation_reversion_observe_v1_enabled = true,
          banking_pay_cancellation_reversion_publish_v1_enabled = true,
          banking_pay_scheduled_cancellation_reversion_v2_observe_enabled = true,
          banking_pay_scheduled_cancellation_reversion_v2_publish_enabled = true,
          banking_pay_workbench_semantic_ready_observe_v2_enabled = true,
          banking_pay_workbench_semantic_ready_publication_v3_enabled = true,
          banking_pay_workbench_semantic_ready_draft_guard_v2_enabled = true,
          banking_pay_selection_intent_identity_v1_enabled = true
      WHERE id = 1
    `);
    const requestedChannel = String(process.env.H2_CANCEL_CHANNEL || '').trim().toUpperCase();
    const channels = requestedChannel ? [requestedChannel] : ['PAYE', 'UMBRELLA'];
    assert.ok(channels.every((channel) => channel === 'PAYE' || channel === 'UMBRELLA'));
    const paymentState = String(process.env.H2_CANCEL_PAYMENT_STATE || 'DRAFT').trim().toUpperCase();
    assert.ok([
      'DRAFT',
      'SCHEDULED_LOCAL_NOT_SENT',
      'IMMEDIATE_FAILED_NO_MONEY',
      'SCHEDULED_FAILED_NO_MONEY'
    ].includes(paymentState));
    const scheduledPreparation = {};
    const results = {
      _fixture_isolation: {
        ...scaleFinancialNormalization,
        ...preexistingWorkbenchIsolation
      },
      _payment_state_fixture: {
        payment_state: paymentState,
        scheduled_preparation: scheduledPreparation
      }
    };
    for (const channel of channels) {
      try {
        results[channel] = runCancellation(target, channel, target.batches[channel]);
        if (results[channel].scheduled_preparation) {
          scheduledPreparation[channel] = results[channel].scheduled_preparation;
        }
      } catch (error) {
        const diagnostics = readFailureDiagnostics(target, error.requestId, error.operationId);
        const postCommitAuthority = diagnosticWrapperInstalled
          ? readPostCommitAuthorityDiagnostics(target, error.requestId, error.operationId)
          : null;
        const financialCancellationApplied = diagnostics
          && diagnostics.request_status === 'APPLIED'
          && diagnostics.batch_status === 'CANCELLED'
          && diagnostics.active_item_count === 0
          && diagnostics.total_work_item_count > 0
          && diagnostics.applied_work_item_count === diagnostics.total_work_item_count
          && diagnostics.provider_attempt_count === 0
          && diagnostics.provider_event_count === 0;
        results[channel] = {
          status: financialCancellationApplied
            ? 'CANCELLATION_APPLIED_REFRESH_FIXTURE_NOT_CURRENT'
            : 'FAILED',
          code: error.code || 'UNCLASSIFIED_FAILURE',
          phase: error.phase || null,
          payload: error.payload || null,
          phase_calls: error.phaseCalls || [],
          financial_cancellation_applied: Boolean(financialCancellationApplied),
          post_commit_authority_diagnostic: postCommitAuthority,
          diagnostics,
          message: String(error.stack || error.message || error)
        };
      }
    }
    return results;
  } finally {
    if (String(process.env.H2_CANCEL_KEEP_DATABASE || '').trim().toLowerCase() !== 'true') {
      dropDatabase(target);
    }
  }
}

const requestedTarget = String(process.env.H2_CANCEL_TARGET || '').trim().toUpperCase();
const availableTargets = matrices.flatMap((matrix) => [
  { name: `${matrix.name}_V1`, postgresMajor: matrix.name, route: 'v1', target: matrix.v1 },
  { name: `${matrix.name}_V8`, postgresMajor: matrix.name, route: 'v8', target: matrix.v8 }
]);
if (requestedTarget) {
  const selected = availableTargets.find((entry) => entry.name === requestedTarget);
  assert.ok(selected, `unknown H2_CANCEL_TARGET ${requestedTarget}`);
  const targetResults = runTarget(selected.target);
  const compactOutput = String(process.env.H2_CANCEL_COMPACT_OUTPUT || '').trim().toLowerCase() === 'true';
  const compactFailure = (result) => {
    let structured = null;
    try {
      structured = JSON.parse(String(result.message || ''));
    } catch {
      structured = null;
    }
    return {
      status: result.status,
      code: result.code,
      phase: result.phase,
      financial_cancellation_applied: result.financial_cancellation_applied,
      diagnostics: result.diagnostics,
      failure_boundary: structured?.code || null,
      retry_evidence: structured?.evidence || null,
      currentness: structured?.currentness || null,
      safe_step_count: Array.isArray(structured?.safe_steps) ? structured.safe_steps.length : null,
      message: structured ? null : String(result.message || '').slice(0, 1000)
    };
  };
  console.log(JSON.stringify({
    contract: 'BANKING_PAY_DRAFT_CANCELLATION_DIAGNOSTIC_V1',
    status: 'DIAGNOSTIC_ONLY',
    target: selected.name,
    replacement_installed: String(process.env.H2_CANCEL_APPLY_REPLACEMENT || '').trim().toLowerCase() === 'true',
    bank_event_classification_constraint_installed: String(process.env.H2_CANCEL_APPLY_BANK_EVENT_CLASSIFICATION || '').trim().toLowerCase() === 'true',
    no_money_unwind_result_arity_installed: String(process.env.H2_CANCEL_APPLY_NO_MONEY_RESULT_ARITY || '').trim().toLowerCase() === 'true',
    set_page_enabled: String(process.env.H2_CANCEL_SET_PAGE || '').trim().toLowerCase() === 'true',
    results: compactOutput
      ? Object.fromEntries(Object.entries(targetResults).map(([channel, result]) => [channel, result.normalized
        ? {
            status: 'PASS',
            normalized: result.normalized,
            digest_sha256: result.digest_sha256,
            operation_iteration_count: result.operation_iteration_count,
            phase_call_count: result.phase_call_count,
            maximum_call_ms: result.maximum_call_ms
          }
        : compactFailure(result)]))
      : targetResults,
    timeout_relaxation: false,
    enforced_statement_timeout_ms: 15000,
    enforced_lock_timeout_ms: 1500,
    provider_payment_settlement_remittance_actions: 0,
    containment: 'TASK_OWNED_DISPOSABLE_DATABASE_CLONE_REMOVED_AFTER_PROOF'
  }, null, 2));
  process.exit(0);
}

const results = [];
for (const matrix of matrices) {
  const v1 = runTarget(matrix.v1);
  const v8 = runTarget(matrix.v8);
  for (const channel of ['PAYE', 'UMBRELLA']) {
    assert.ok(v8[channel].normalized, `${matrix.name} ${channel} V8 cancellation did not complete: ${JSON.stringify(v8[channel])}`);
  }
  results.push({
    postgres_major: matrix.name,
    v1,
    v8,
    historical_v1_runtime_outcome: 'RECORDED_WITHOUT_USING_A_FAILED_ROUTE_AS_THE_POLICY_ORACLE',
    current_v8_runtime_outcome: 'PASS'
  });
}

for (const channel of ['PAYE', 'UMBRELLA']) {
  assert.deepEqual(results[0].v8[channel].normalized, results[1].v8[channel].normalized, `${channel} PostgreSQL 17/18 parity mismatch`);
}

console.log(JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V1_V8_WHOLE_DRAFT_CANCELLATION_COMPENSATED_PARITY_V1',
  status: 'PASS',
  results,
  historical_v1_failure_compensated_by_frozen_policy_invariants: true,
  exact_postgres_17_18_parity: true,
  timeout_relaxation: false,
  provider_payment_settlement_remittance_actions: 0,
  containment: 'TASK_OWNED_DISPOSABLE_DATABASE_CLONES_REMOVED_AFTER_PROOF'
}, null, 2));
