import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, '..');
const fixturePath = path.join(
  repoRoot,
  'tests',
  'fixtures',
  '07092026_2130_h12_source_less_cancellation_runtime_v1.json'
);
const setupSqlPath = path.join(
  repoRoot,
  'tests',
  '07092026_2131_h12_source_less_cancellation_runtime_verification.sql'
);
const baseRunnerPath = path.join(
  repoRoot,
  'scripts',
  'verify-banking-pay-draft-v1-v8-cancellation-parity.mjs'
);
const resultDirectory = path.join(
  repoRoot,
  'codex_outputs',
  'h12-banking-draft-v8'
);

const BASE_RUNNER_SHA256 = 'd783120af8fb86ed72fc277e09b5b828a1c6f1c8069cb9151e53f26c14feeade';
const FRESH_COMMUNICATION_PREPARE_PATH = 'supabase/repeatable/07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql';
const FRESH_COMMUNICATION_EXPAND_PATH = 'supabase/repeatable/07092026_2014_banking_pay_unpaid_cancellation_communication_v2_expand_v1.sql';
const VERSIONED_INTEGRITY_CHECKER_PATH = 'supabase/repeatable/07092026_2021_banking_pay_payment_correction_integrity_communication_v2_v1.sql';
const SOURCE_LESS_APPLY_PATH = 'supabase/repeatable/07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql';
const STATUS_PAGE_ADMISSION_PATH = 'supabase/repeatable/07092026_2135_banking_pay_unpaid_cancellation_status_admission_v1.sql';
const MANUAL_CARRY_FORWARD_IDENTITY_PATH = 'supabase/repeatable/07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql';
const MANUAL_CARRY_FORWARD_IDENTITY_SHA256 = 'b4ec1a479c0608f997daf0de9d2816803b9aa3af6f3a09edfd411c71572c3e4f';
const CANDIDATE_DIRTY_COHORT_PATH = 'supabase/repeatable/08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql';
const SCOPE_INVALIDATOR_PAIR_ARRAYS_PATH = 'supabase/repeatable/08092026_0804_pay_workbench_scope_invalidate_pair_arrays_v1.sql';
const CANCEL_RETURN_FROZEN_SCOPE_INDEX_PATH = 'supabase/migrations/08092026_1159_banking_pay_cancel_return_frozen_scope_lookup_v1.sql';
const CANCEL_RETURN_SELECTION_INTENT_PATH = 'supabase/repeatable/08092026_1200_banking_pay_cancel_return_selection_intent_v1.sql';
const CERTIFIED_PREVIEW_FINAL_SELECTION_COUNT_PATH = 'supabase/repeatable/08092026_1201_banking_pay_certified_preview_final_selection_count_v1.sql';
const NO_MONEY_WORKBENCH_RETURN_PATH = 'supabase/repeatable/09092026_0020_banking_pay_no_money_workbench_return_v1.sql';
const SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH = 'supabase/repeatable/04082026_1139_pay_workbench_scope_invalidate_v1.sql';
const SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_SHA256 = 'f304e2d072d9c93f8fbe1e4ab9998b64d926a161c7b6ef4bde86dcb3ca681538';
const CANCELLATION_NOTICE_MIGRATION_PATH = 'supabase/migrations/07092026_2300_banking_pay_payment_cancellation_mail_outbox_v1.sql';
const CANCELLATION_NOTICE_OWNER_PATH = 'supabase/repeatable/07092026_2301_banking_pay_payment_cancellation_notice_reconcile_v1.sql';
const CANCELLATION_NOTICE_SCALE_MIGRATION_SHA256 = '4902b4bf030b41f0e223a5b9db6e00113e0d3320066a8598c42822ea86c13a94';
const CANCELLATION_NOTICE_SCALE_OWNER_SHA256 = '682445ae51d19f04a82eeae239d2b75e95d477872e703a3bdf1b69b9677b2189';
const SOURCE_LESS_NO_MONEY_RESULT_KEYSET_SHA256 = '97ef9838f2003cae69da7f539a65d572a069ca01a20d9f0a96a5580685b69bdd';
const SOURCE_LESS_NO_MONEY_PERSISTED_WRAPPER_KEYS = Object.freeze([
  'candidate_scope_hash',
  'created_by',
  'selection_ordinal'
]);
const ACTOR_ID = '10000000-0000-4000-8000-000000000001';
const SOURCE_DATABASE = 'banking_modal_v2_test';
const MODE = String(process.env.H12_SOURCE_LESS_MODE || 'CURRENT_RED').trim().toUpperCase();
const DESCRIBE_ONLY = process.argv.includes('--describe');
const CHECK_TRANSFORM_ONLY = process.argv.includes('--check-transform');
const CHECK_RESULT_PATH_ONLY = process.argv.includes('--check-result-path');
const SNAPSHOT_EQUIVALENCE_ONLY = process.argv.includes('--snapshot-equivalence');
const MAIL_MULTICANDIDATE_SCALE_ONLY = process.argv.includes('--mail-multicandidate-scale');
const KEEP_TRANSFORMED_RUNNER = String(
  process.env.H12_SOURCE_LESS_KEEP_TRANSFORMED_RUNNER || 'false'
).trim().toLowerCase() === 'true';
const KEEP_FAILED_TARGET_DATABASE = String(
  process.env.H12_SOURCE_LESS_KEEP_DATABASE || 'false'
).trim().toLowerCase() === 'true';
const REUSE_VERIFIED_PREPARED_SNAPSHOT = String(
  process.env.H12_SOURCE_LESS_REUSE_VERIFIED_PREPARED_SNAPSHOT || 'false'
).trim().toLowerCase() === 'true';
const ESTABLISHED_CURRENT_BASELINE_FLAGS = Object.freeze({
  H2_CANCEL_APPLY_REPLACEMENT: 'true',
  H2_CANCEL_APPLY_ONE_CANDIDATE_INTEGRITY: 'true',
  H2_CANCEL_APPLY_RESERVATION_EVIDENCE_PRECEDENCE: 'true',
  H2_CANCEL_APPLY_BANK_EVENT_CLASSIFICATION: 'true',
  H2_CANCEL_APPLY_NO_MONEY_RESULT_ARITY: 'true',
  H2_CANCEL_APPLY_COMPLETION_AUDIT: 'true',
  H2_CANCEL_APPLY_ROUTE_HANDOFF: 'false'
});
let sourceLessNoMoneyResultKeys = Object.freeze([]);

const engines = {
  PG17: {
    container: 'h12-v8-restart-pg17',
    templateDatabase: String(
      process.env.H12_SOURCE_LESS_PG17_TEMPLATE_DATABASE || 'h12_sourceless_baseline_pg17'
    ).trim(),
    preparedTemplateDatabase: String(
      process.env.H12_SOURCE_LESS_PG17_PREPARED_TEMPLATE_DATABASE || 'h12_rg5_prepared_pg17'
    ).trim(),
    target: 'PG17_V8',
    targetDatabase: String(
      process.env.H12_SOURCE_LESS_PG17_TARGET_DATABASE || 'h2_cancel_v8_pg17'
    ).trim(),
    batches: {
      PAYE: 'fda06a34-168f-468f-98b8-e9f3fe9d029d',
      UMBRELLA: '664390b3-6547-4c28-9af2-4baf0c6d7cdb'
    }
  },
  PG18: {
    container: 'h12-v8-restart-pg18',
    templateDatabase: String(
      process.env.H12_SOURCE_LESS_PG18_TEMPLATE_DATABASE || 'h12_sourceless_baseline_pg18'
    ).trim(),
    preparedTemplateDatabase: String(
      process.env.H12_SOURCE_LESS_PG18_PREPARED_TEMPLATE_DATABASE || 'h12_rg5_prepared_pg18'
    ).trim(),
    target: 'PG18_V8',
    targetDatabase: String(
      process.env.H12_SOURCE_LESS_PG18_TARGET_DATABASE || 'h2_cancel_v8_pg18'
    ).trim(),
    batches: {
      PAYE: 'd8a00679-fd2f-4def-8a1f-f0350ca597a4',
      UMBRELLA: '1d97a6a4-706f-4ec7-a8a4-98f03a7468c2'
    }
  }
};

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function replaceExactlyOnce(source, before, after, label) {
  assert.equal(source.split(before).length - 1, 1, `${label} splice changed`);
  return source.replace(before, after);
}

function run(command, args, { allowFailure = false, input = undefined } = {}) {
  const startedAt = performance.now();
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    input,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true
  });
  const elapsedMs = Number((performance.now() - startedAt).toFixed(3));
  if (!allowFailure && result.status !== 0) {
    throw new Error([
      `${command} failed with status ${result.status}`,
      result.stdout,
      result.stderr
    ].filter(Boolean).join('\n'));
  }
  return { ...result, elapsedMs };
}

function docker(container, args, options = {}) {
  return run('docker', ['exec', container, ...args], options);
}

function applySqlFileToDatabase(engine, database, relativePath) {
  const absolutePath = path.join(repoRoot, ...relativePath.split('/'));
  assert.equal(fs.existsSync(absolutePath), true, `SQL source missing: ${relativePath}`);
  const containerPath = `/tmp/${path.basename(relativePath)}`;
  const copied = run('docker', ['cp', absolutePath, `${engine.container}:${containerPath}`]);
  assert.equal(copied.status, 0);
  try {
    docker(engine.container, [
      'env',
      'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
      'psql', '-U', 'postgres', '-d', database,
      '-X', '-v', 'ON_ERROR_STOP=1', '-f', containerPath
    ]);
  } finally {
    docker(engine.container, ['rm', '-f', containerPath], { allowFailure: true });
  }
}

function queryDatabaseJson(engine, database, sql) {
  const result = docker(engine.container, [
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', database,
    '-X', '-A', '-t', '-q', '-v', 'ON_ERROR_STOP=1', '-c', sql
  ]);
  const lines = String(result.stdout || '')
    .trim()
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  assert.ok(lines.length > 0, `query returned no JSON: ${result.stderr}`);
  return JSON.parse(lines.at(-1));
}

function runMailMulticandidateScale(engineName, engine) {
  const migrationAbsolute = path.join(repoRoot, ...CANCELLATION_NOTICE_MIGRATION_PATH.split('/'));
  const ownerAbsolute = path.join(repoRoot, ...CANCELLATION_NOTICE_OWNER_PATH.split('/'));
  assert.equal(sha256(fs.readFileSync(migrationAbsolute)), CANCELLATION_NOTICE_SCALE_MIGRATION_SHA256);
  assert.equal(sha256(fs.readFileSync(ownerAbsolute)), CANCELLATION_NOTICE_SCALE_OWNER_SHA256);

  prepareTemplateDatabase(engine);
  docker(engine.container, ['dropdb', '-U', 'postgres', '--if-exists', engine.targetDatabase]);
  try {
    docker(engine.container, [
      'createdb', '-U', 'postgres', '-T', engine.templateDatabase, engine.targetDatabase
    ]);
    applySqlFileToDatabase(engine, engine.targetDatabase, CANCELLATION_NOTICE_MIGRATION_PATH);
    applySqlFileToDatabase(engine, engine.targetDatabase, CANCELLATION_NOTICE_OWNER_PATH);

    const setupSql = String.raw`
      SET client_min_messages TO warning;
      BEGIN;
      CREATE SCHEMA h12_runtime;
      CREATE TABLE h12_runtime.scale_context (
        request_id uuid PRIMARY KEY,
        pay_batch_id uuid NOT NULL,
        candidate_count integer NOT NULL
      );

      WITH selected_batch AS (
        SELECT batch_row.id
        FROM public.pay_batches AS batch_row
        WHERE batch_row.batch_kind_fixed = 'PAYE'
        ORDER BY batch_row.id
        LIMIT 1
      )
      INSERT INTO h12_runtime.scale_context(request_id, pay_batch_id, candidate_count)
      SELECT pg_catalog.md5(
               'h12-mail-multicandidate-request:' || pg_catalog.current_database()
             )::uuid,
             selected_batch.id,
             5000
      FROM selected_batch;

      DO $guard$
      BEGIN
        IF (SELECT pg_catalog.count(*) FROM h12_runtime.scale_context) <> 1 THEN
          RAISE EXCEPTION USING MESSAGE = 'H12_MULTICANDIDATE_SOURCE_BATCH_MISSING';
        END IF;
      END
      $guard$;

      INSERT INTO public.candidates (
        id, first_name, last_name, display_name, pay_method, active
      )
      SELECT pg_catalog.md5(
               'h12-mail-multicandidate-candidate:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             'H12',
             'Mail scale ' || generated.ordinal::text,
             'H12 Mail scale ' || generated.ordinal::text,
             'PAYE',
             true
      FROM pg_catalog.generate_series(1, 5000) AS generated(ordinal);

      INSERT INTO public.pay_batch_candidates (
        id, pay_batch_id, candidate_id, candidate_display_name,
        gross_preview, net_bank_amount
      )
      SELECT pg_catalog.md5(
               'h12-mail-multicandidate-pbc:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             context.pay_batch_id,
             pg_catalog.md5(
               'h12-mail-multicandidate-candidate:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             'H12 Mail scale ' || generated.ordinal::text,
             1.00,
             1.00
      FROM h12_runtime.scale_context AS context
      CROSS JOIN pg_catalog.generate_series(1, 5000) AS generated(ordinal);

      INSERT INTO public.pay_batch_items (
        id, pay_batch_candidate_id, item_type, description,
        amount_ex_vat, amount_vat, amount_inc_vat, pay_channel, is_voided
      )
      SELECT pg_catalog.md5(
               'h12-mail-multicandidate-item:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             pg_catalog.md5(
               'h12-mail-multicandidate-pbc:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             'TS_TOTAL',
             'H12 scale item ' || generated.ordinal::text,
             1.00,
             0.00,
             1.00,
             'PAYE',
             true
      FROM pg_catalog.generate_series(1, 5000) AS generated(ordinal);

      INSERT INTO public.pay_payment_correction_requests (
        id, pay_batch_id, correction_kind, status, required_quantity,
        approved_count, golden_key_used, selection_json, selection_hash,
        plan_json, plan_hash, auto_requested, created_at_utc, applied_at_utc,
        updated_at_utc, cancel_notice_tracked
      )
      SELECT context.request_id,
             context.pay_batch_id,
             'PRE_BANK_CANCEL',
             'APPLIED',
             5000,
             5000,
             false,
             '{}'::jsonb,
             pg_catalog.repeat('a', 64),
             pg_catalog.jsonb_build_object(
               'candidate_scope_contract_version', '2',
               'candidate_scope_hash_version', '2',
               'source_row_count_semantics', 'FINANCIAL_ONLY',
               'communication_cleanup_contract_version', '2'
             ),
             pg_catalog.repeat('b', 64),
             false,
             '2026-09-08 03:00:00+00'::timestamptz,
             '2026-09-08 03:01:00+00'::timestamptz,
             '2026-09-08 03:01:00+00'::timestamptz,
             true
      FROM h12_runtime.scale_context AS context;

      INSERT INTO public.pay_payment_correction_items (
        id, correction_request_id, pay_batch_id, pay_batch_candidate_id,
        candidate_id, pay_batch_item_id, correction_item_kind, status,
        created_at_utc, applied_at_utc
      )
      SELECT pg_catalog.md5(
               'h12-mail-multicandidate-correction-item:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             context.request_id,
             context.pay_batch_id,
             pg_catalog.md5(
               'h12-mail-multicandidate-pbc:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             pg_catalog.md5(
               'h12-mail-multicandidate-candidate:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             pg_catalog.md5(
               'h12-mail-multicandidate-item:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             'PRE_BANK_CANCEL',
             'APPLIED',
             '2026-09-08 03:00:30+00'::timestamptz,
             '2026-09-08 03:01:00+00'::timestamptz
      FROM h12_runtime.scale_context AS context
      CROSS JOIN pg_catalog.generate_series(1, 5000) AS generated(ordinal);

      INSERT INTO public.mail_outbox (
        id, type, "to", subject, attachments, status, created_at_utc,
        sent_at, reference, recipient_kind, recipient_id, context_kind,
        context_id, provider_status, payment_scope_json,
        deterministic_outbox_key, attachments_ready,
        attachment_total_bytes, cancel_notice_tracked
      )
      SELECT pg_catalog.md5(
               'h12-mail-multicandidate-source-mail:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             'REMITTANCE',
             'h12-scale-' || generated.ordinal::text || '@example.invalid',
             'H12 source-valid Candidate remittance',
             '[]'::jsonb,
             'SENT'::public.mail_status_enum,
             '2026-09-08 03:02:00+00'::timestamptz +
               ((generated.ordinal * 2 - (generated.ordinal % 2))::text ||
                ' microseconds')::interval,
             '2026-09-08 03:02:30+00'::timestamptz +
               ((generated.ordinal * 2 - (generated.ordinal % 2))::text ||
                ' microseconds')::interval,
             'h12-scale-remittance:' || generated.ordinal::text,
             'candidate',
             pg_catalog.md5(
               'h12-mail-multicandidate-candidate:' ||
               pg_catalog.current_database() || ':' || generated.ordinal::text
             )::uuid,
             'pay_batches',
             context.pay_batch_id,
             'ACCEPTED',
             pg_catalog.jsonb_build_object(
               'remittance_type', 'CANDIDATE_REMITTANCE',
               'pay_batch_id', context.pay_batch_id,
               'candidate_id', pg_catalog.md5(
                 'h12-mail-multicandidate-candidate:' ||
                 pg_catalog.current_database() || ':' || generated.ordinal::text
               )::uuid,
               'pay_batch_candidate_id', pg_catalog.md5(
                 'h12-mail-multicandidate-pbc:' ||
                 pg_catalog.current_database() || ':' || generated.ordinal::text
               )::uuid,
               'pay_batch_item_ids', pg_catalog.jsonb_build_array(
                 pg_catalog.md5(
                   'h12-mail-multicandidate-item:' ||
                   pg_catalog.current_database() || ':' || generated.ordinal::text
                 )::uuid::text
               ),
               'item_count', 1
             ),
             'h12-scale-source:' || pg_catalog.current_database() || ':' ||
               generated.ordinal::text,
             true,
             0,
             true
      FROM h12_runtime.scale_context AS context
      CROSS JOIN pg_catalog.generate_series(1, 5000) AS generated(ordinal);

      CREATE TABLE h12_runtime.side_effect_before AS
      SELECT
        (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_provider_attempts) AS provider_attempt_count,
        (SELECT pg_catalog.count(*) FROM public.pay_bank_transfer_events) AS provider_event_count,
        (SELECT pg_catalog.count(*) FROM public.pay_bank_transfers) AS transfer_count,
        (SELECT pg_catalog.md5(pg_catalog.string_agg(pg_catalog.md5(pg_catalog.to_jsonb(item_row)::text), '' ORDER BY item_row.id))
           FROM public.pay_batch_items AS item_row
           WHERE item_row.id IN (
             SELECT pg_catalog.md5(
               'h12-mail-multicandidate-item:' || pg_catalog.current_database() || ':' ||
               generated.ordinal::text
             )::uuid
             FROM pg_catalog.generate_series(1, 5000) AS generated(ordinal)
           )) AS item_digest,
        (SELECT pg_catalog.md5(pg_catalog.string_agg(pg_catalog.md5(pg_catalog.to_jsonb(correction_row)::text), '' ORDER BY correction_row.id))
           FROM public.pay_payment_correction_items AS correction_row
           JOIN h12_runtime.scale_context AS context
             ON context.request_id = correction_row.correction_request_id) AS correction_item_digest;

      CREATE FUNCTION h12_runtime.call_notice_page(p_request_id uuid)
      RETURNS jsonb
      LANGUAGE plpgsql
      SET search_path TO ''
      SET statement_timeout TO '6000ms'
      SET lock_timeout TO '1000ms'
      AS $call$
      DECLARE
        v_started_at timestamptz := pg_catalog.clock_timestamp();
        v_result jsonb;
      BEGIN
        v_result := public.pay_payment_cancellation_notice_reconcile_v1(
          p_correction_request_id => p_request_id,
          p_limit => 100
        );
        RETURN v_result || pg_catalog.jsonb_build_object(
          '_database_elapsed_ms',
          pg_catalog.round(
            extract(epoch FROM pg_catalog.clock_timestamp() - v_started_at) * 1000,
            3
          )
        );
      END
      $call$;

      ANALYZE public.pay_payment_correction_items;
      ANALYZE public.mail_outbox;
      COMMIT;
    `;
    run('docker', [
      'exec', '-i', engine.container,
      'env',
      'PGOPTIONS=-c statement_timeout=30s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=60s -c jit=off',
      'psql', '-U', 'postgres', '-d', engine.targetDatabase,
      '-X', '-v', 'ON_ERROR_STOP=1'
    ], { input: setupSql });

    const plan = docker(engine.container, [
      'env',
      'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c jit=off',
      'psql', '-U', 'postgres', '-d', engine.targetDatabase,
      '-X', '-A', '-t', '-q', '-v', 'ON_ERROR_STOP=1', '-c', String.raw`
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        WITH request_scope AS MATERIALIZED (
          SELECT DISTINCT correction_item.candidate_id::text AS candidate_id,
                          correction_item.pay_batch_candidate_id::text AS pay_batch_candidate_id
          FROM public.pay_payment_correction_items AS correction_item
          JOIN h12_runtime.scale_context AS context
            ON context.request_id = correction_item.correction_request_id
          WHERE correction_item.pay_batch_id = context.pay_batch_id
            AND correction_item.candidate_id IS NOT NULL
            AND correction_item.pay_batch_candidate_id IS NOT NULL
            AND correction_item.correction_item_kind = 'PRE_BANK_CANCEL'
            AND correction_item.status = 'APPLIED'
            AND correction_item.applied_at_utc IS NOT NULL
          ORDER BY correction_item.candidate_id::text,
                   correction_item.pay_batch_candidate_id::text
          LIMIT 50001
        )
        SELECT source_mail.id, source_mail.created_at_utc
        FROM request_scope
        JOIN h12_runtime.scale_context AS context ON true
        JOIN public.mail_outbox AS source_mail
          ON source_mail.context_id = context.pay_batch_id
         AND source_mail.payment_scope_json->>'candidate_id' = request_scope.candidate_id
         AND source_mail.payment_scope_json->>'pay_batch_candidate_id' = request_scope.pay_batch_candidate_id
        WHERE source_mail.context_kind = 'pay_batches'
          AND source_mail.type = 'REMITTANCE'
          AND source_mail.status = 'SENT'::public.mail_status_enum
          AND source_mail.sent_at IS NOT NULL
          AND source_mail.provider_status = 'ACCEPTED'
        ORDER BY source_mail.created_at_utc, source_mail.id
        LIMIT 101;
      `
    ]);
    const planText = String(plan.stdout || '');
    const requiredIndex = 'mail_outbox_payment_cancellation_sent_candidate_source_idx';
    const planUsesCandidateSourceIndex = planText.includes(requiredIndex);
    const planExecutionMatch = planText.match(/Execution Time:\s*([0-9.]+)\s*ms/i);
    assert.ok(planExecutionMatch, planText);
    assert.match(planText, /(?:Index|Seq) Scan on mail_outbox/i, planText);

    const context = queryDatabaseJson(engine, engine.targetDatabase, String.raw`
      SELECT pg_catalog.jsonb_build_object(
        'request_id', context.request_id,
        'candidate_count', context.candidate_count
      )::text
      FROM h12_runtime.scale_context AS context
    `);
    assert.equal(context.candidate_count, 5000);

    const pages = [];
    for (let pageNumber = 1; pageNumber <= 50; pageNumber += 1) {
      const page = queryDatabaseJson(engine, engine.targetDatabase, `
        SELECT h12_runtime.call_notice_page('${context.request_id}'::uuid)::text
      `);
      assert.equal(page.ok, true, JSON.stringify(page));
      assert.equal(page.template_version, 'PAYMENT_CANCELLATION_NOTICE_V1');
      assert.equal(page.progress_owner, 'SERVER_ROW');
      assert.equal(page.next_cursor, null);
      assert.equal(page.examined, 100, JSON.stringify(page));
      assert.equal(page.eligible, 100, JSON.stringify(page));
      assert.equal(page.queued, 100, JSON.stringify(page));
      assert.equal(page.already_present, 0, JSON.stringify(page));
      assert.equal(page.skipped, 0, JSON.stringify(page));
      assert.deepEqual(page.reason_counts, {});
      assert.equal(page.has_more, pageNumber < 50, JSON.stringify(page));
      assert.ok(page._database_elapsed_ms < 6000, JSON.stringify(page));
      pages.push(page);
    }

    const terminalReplay = queryDatabaseJson(engine, engine.targetDatabase, `
      SELECT h12_runtime.call_notice_page('${context.request_id}'::uuid)::text
    `);
    assert.equal(terminalReplay.ok, true);
    assert.equal(terminalReplay.already_complete, true);
    assert.equal(terminalReplay.examined, 0);
    assert.equal(terminalReplay.eligible, 0);
    assert.equal(terminalReplay.queued, 0);
    assert.equal(terminalReplay.already_present, 0);
    assert.equal(terminalReplay.skipped, 0);
    assert.deepEqual(terminalReplay.reason_counts, {});
    assert.equal(terminalReplay.has_more, false);
    assert.equal(terminalReplay.next_cursor, null);
    assert.ok(terminalReplay._database_elapsed_ms < 6000);

    const finalState = queryDatabaseJson(engine, engine.targetDatabase, String.raw`
      WITH context AS (
        SELECT * FROM h12_runtime.scale_context
      ), before_state AS (
        SELECT * FROM h12_runtime.side_effect_before
      )
      SELECT pg_catalog.jsonb_build_object(
        'notice_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.mail_outbox AS notice_row
          WHERE notice_row.type = 'PAYMENT_CANCELLATION'
            AND notice_row.context_kind = 'pay_payment_correction_requests'
            AND notice_row.context_id = context.request_id
        ),
        'distinct_notice_key_count', (
          SELECT pg_catalog.count(DISTINCT notice_row.deterministic_outbox_key)::integer
          FROM public.mail_outbox AS notice_row
          WHERE notice_row.type = 'PAYMENT_CANCELLATION'
            AND notice_row.context_kind = 'pay_payment_correction_requests'
            AND notice_row.context_id = context.request_id
        ),
        'source_reconciled_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.mail_outbox AS source_row
          WHERE source_row.deterministic_outbox_key LIKE
                  'h12-scale-source:' || pg_catalog.current_database() || ':%'
            AND source_row.cancel_notice_reconciled_sent_at_utc = source_row.sent_at
            AND source_row.cancel_notice_result_code = 'QUEUED'
        ),
        'request_result', request_row.cancel_notice_result_json,
        'request_terminal_token_equal',
          request_row.cancel_notice_reconciled_applied_at_utc = request_row.applied_at_utc,
        'request_progress_cleared',
          request_row.cancel_notice_progress_applied_at_utc IS NULL
          AND request_row.cancel_notice_after_created_at_utc IS NULL
          AND request_row.cancel_notice_after_mail_outbox_id IS NULL
          AND request_row.cancel_notice_next_attempt_at_utc IS NULL,
        'provider_attempt_delta',
          (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_provider_attempts) -
          before_state.provider_attempt_count,
        'provider_event_delta',
          (SELECT pg_catalog.count(*) FROM public.pay_bank_transfer_events) -
          before_state.provider_event_count,
        'transfer_delta',
          (SELECT pg_catalog.count(*) FROM public.pay_bank_transfers) - before_state.transfer_count,
        'item_digest_unchanged', before_state.item_digest = (
          SELECT pg_catalog.md5(pg_catalog.string_agg(pg_catalog.md5(pg_catalog.to_jsonb(item_row)::text), '' ORDER BY item_row.id))
          FROM public.pay_batch_items AS item_row
          WHERE item_row.id IN (
            SELECT pg_catalog.md5(
              'h12-mail-multicandidate-item:' || pg_catalog.current_database() || ':' ||
              generated.ordinal::text
            )::uuid
            FROM pg_catalog.generate_series(1, 5000) AS generated(ordinal)
          )
        ),
        'correction_item_digest_unchanged', before_state.correction_item_digest = (
          SELECT pg_catalog.md5(pg_catalog.string_agg(pg_catalog.md5(pg_catalog.to_jsonb(correction_row)::text), '' ORDER BY correction_row.id))
          FROM public.pay_payment_correction_items AS correction_row
          WHERE correction_row.correction_request_id = context.request_id
        )
      )::text
      FROM context
      JOIN public.pay_payment_correction_requests AS request_row
        ON request_row.id = context.request_id
      CROSS JOIN before_state
    `);
    assert.equal(finalState.notice_count, 5000, JSON.stringify(finalState));
    assert.equal(finalState.distinct_notice_key_count, 5000, JSON.stringify(finalState));
    assert.equal(finalState.source_reconciled_count, 5000, JSON.stringify(finalState));
    assert.equal(finalState.request_terminal_token_equal, true, JSON.stringify(finalState));
    assert.equal(finalState.request_progress_cleared, true, JSON.stringify(finalState));
    assert.equal(finalState.request_result.examined, 5000, JSON.stringify(finalState));
    assert.equal(finalState.request_result.eligible, 5000, JSON.stringify(finalState));
    assert.equal(finalState.request_result.queued, 5000, JSON.stringify(finalState));
    assert.equal(finalState.request_result.complete, true, JSON.stringify(finalState));
    assert.equal(finalState.provider_attempt_delta, 0, JSON.stringify(finalState));
    assert.equal(finalState.provider_event_delta, 0, JSON.stringify(finalState));
    assert.equal(finalState.transfer_delta, 0, JSON.stringify(finalState));
    assert.equal(finalState.item_digest_unchanged, true, JSON.stringify(finalState));
    assert.equal(finalState.correction_item_digest_unchanged, true, JSON.stringify(finalState));

    const elapsed = pages.map((page) => page._database_elapsed_ms);
    return {
      engine: engineName,
      status: 'PASS',
      candidate_count: 5000,
      pay_batch_candidate_count: 5000,
      item_count: 5000,
      sent_source_mail_count: 5000,
      page_limit: 100,
      page_count: pages.length,
      first_page_ms: elapsed[0],
      middle_page_ms: elapsed[Math.floor(elapsed.length / 2)],
      final_page_ms: elapsed.at(-1),
      maximum_page_ms: Math.max(...elapsed),
      terminal_replay_ms: terminalReplay._database_elapsed_ms,
      plan_uses_candidate_source_index: planUsesCandidateSourceIndex,
      plan_mail_access: planUsesCandidateSourceIndex
        ? 'INDEX_SCAN'
        : 'SEQUENTIAL_SCAN_ALL_5000_ROWS_RELEVANT',
      plan_execution_ms: Number(planExecutionMatch[1]),
      required_index: requiredIndex,
      exact_notice_count: finalState.notice_count,
      provider_settlement_remittance_delta: 0,
      owner_sha256: CANCELLATION_NOTICE_SCALE_OWNER_SHA256
    };
  } finally {
    docker(engine.container, ['dropdb', '-U', 'postgres', '--if-exists', engine.targetDatabase], { allowFailure: true });
    dropTemplateDatabase(engine);
  }
}

function validateFixture(fixture) {
  assert.equal(fixture.artifact, 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_MATRIX_V1');
  assert.equal(fixture.matrix.logical_case_count, 40);
  assert.equal(fixture.matrix.physical_run_count, 16);
  assert.deepEqual(fixture.matrix.axes.engines, ['PG17', 'PG18']);
  assert.deepEqual(fixture.matrix.axes.routes, ['PRE_BANK', 'NO_MONEY']);
  assert.deepEqual(fixture.matrix.axes.scopes, ['ONE_CANDIDATE', 'WHOLE_DRAFT']);
  assert.equal(fixture.matrix.axes.reasons.length, 5);
  assert.equal(new Set(fixture.matrix.axes.reasons).size, 5);
  assert.equal(fixture.reason_cases.length, 5);
  assert.deepEqual(
    [...fixture.reason_cases.map((row) => row.reason)].sort(),
    [...fixture.matrix.axes.reasons].sort()
  );
  assert.equal(fixture.policy_boundary.confirmed_unpaid_cancellation_must_complete, true);
  assert.equal(fixture.policy_boundary.ambiguous_adjustment_automatic_carry_forward, false);
  assert.equal(fixture.policy_boundary.ambiguous_adjustment_future_economics_inferred, false);
  assert.equal(fixture.policy_boundary.mail_can_veto_cancellation, false);
  assert.equal(fixture.policy_boundary.timeout_relaxation, false);
  assert.deepEqual(
    fixture.current_red_contract.observed_dual_engine_control.map((row) => row.engine),
    ['PG17', 'PG18']
  );
  assert.ok(fixture.current_red_contract.observed_dual_engine_control.every(
    (row) => row.status === 'EXPECTED_CURRENT_RED_ADMISSION_VETO'
      && row.logical_cell_count === 3
      && row.financial_cancellation_applied === false
  ));
  assert.deepEqual(
    fixture.current_red_contract.route_scope_specific_observed_symptoms.PRE_BANK_ONE_CANDIDATE.required_evidence,
    ['draft_cancel_eligible=false']
  );
  assert.deepEqual(
    fixture.current_red_contract.route_scope_specific_observed_symptoms.PRE_BANK_WHOLE_DRAFT.required_evidence,
    ['PAYMENT_CORRECTION_SELECTION_EMPTY', 'DESCRIPTOR_INVALID']
  );
  assert.deepEqual(
    fixture.current_red_contract.route_scope_specific_observed_symptoms.NO_MONEY_ALL_SCOPES.required_evidence,
    ['rows=[]', 'eligible_matching_count=0']
  );
  assert.equal(
    fixture.conditional_cancellation_follow_up_contract.status,
    'FROZEN_LOCAL_CANDIDATE_RUNTIME_PROOF_PENDING'
  );
  assert.equal(fixture.conditional_cancellation_follow_up_contract.financial_cancellation_dependency, false);
  assert.equal(fixture.conditional_cancellation_follow_up_contract.runs_inside_financial_apply_owner, false);
  assert.equal(fixture.conditional_cancellation_follow_up_contract.zero_follow_up_cells.length, 6);
  assert.equal(fixture.conditional_cancellation_follow_up_contract.retry_and_lost_reply_maximum_follow_up_count, 1);
  assert.equal(fixture.communication_contract_compatibility.total_case_count, 4);
  assert.equal(fixture.communication_contract_compatibility.contract_case_count, 3);
  assert.equal(fixture.communication_contract_compatibility.cutover_guard_case_count, 1);
  assert.deepEqual(
    fixture.communication_contract_compatibility.contracts.map((row) => row.case_id),
    [
      'LEGACY_V1_EXACT_REPLAY',
      'LEGACY_V2_COMMUNICATION_V1_EXACT_REPLAY',
      'FRESH_V2_COMMUNICATION_V2_MAIL_INDEPENDENT'
    ]
  );
  assert.equal(
    fixture.communication_contract_compatibility.contracts[0].expected_behavior,
    'EXACT_HISTORICAL_V1_REPLAY'
  );
  assert.equal(
    fixture.communication_contract_compatibility.contracts[1].expected_behavior,
    'EXACT_HISTORICAL_V2_COMMUNICATION_V1_REPLAY'
  );
  assert.equal(
    fixture.communication_contract_compatibility.contracts[2].communication_cleanup_contract_version,
    2
  );
  assert.equal(
    fixture.communication_contract_compatibility.cutover_guard_cells[0].case_id,
    'NONTERMINAL_OLD_REQUEST_DETECTED_NOT_REINTERPRETED'
  );
  assert.equal(
    fixture.communication_contract_compatibility.cutover_guard_cells[0].release_safety_evidence_only,
    true
  );
  assert.equal(
    fixture.communication_contract_compatibility.cutover_guard_cells[0].ui_retry_claim,
    false
  );
  assert.equal(
    fixture.communication_contract_compatibility.release_precondition.active_nonterminal_v1_or_v2_communication_v1_request_count,
    0
  );
  assert.equal(fixture.communication_contract_compatibility.release_precondition.fail_closed_if_nonzero, true);
  assert.deepEqual(
    fixture.communication_contract_compatibility.release_precondition.nonterminal_request_statuses,
    ['PLANNING', 'PLANNED', 'REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING']
  );
  assert.deepEqual(
    fixture.communication_contract_compatibility.release_precondition.nonterminal_linked_payment_correction_operation_statuses,
    ['QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_PROVIDER', 'REVIEW_REQUIRED']
  );
  assert.equal(
    fixture.communication_contract_compatibility.release_precondition.unexpected_or_null_status_fails_closed,
    true
  );
  assert.deepEqual(fixture.integrity_hash_compatibility.required_exact_hash_contracts, [
    'LEGACY_V1',
    'LEGACY_V2_COMMUNICATION_V1',
    'FRESH_V2_COMMUNICATION_V2'
  ]);
  assert.equal(fixture.integrity_hash_compatibility.invalid_marker_cases.length, 4);
  assert.equal(
    fixture.integrity_hash_compatibility.new_additive_checker_owner_path,
    VERSIONED_INTEGRITY_CHECKER_PATH
  );
  assert.equal(
    fixture.integrity_hash_compatibility.path_status,
    'GENERATOR_AUTHENTIC_PATH_ASSIGNED_LOCAL_CANDIDATE_PROOF_PENDING'
  );
  assert.equal(fixture.integrity_hash_compatibility.payment_policy_scope_expanded, false);
  assert.equal(
    fixture.status_page_admission_reconciliation.classification,
    'CURRENT_PAYMENT_STATUS_SOURCE_LESS_CANCELLATION_ACTION_WITHHELD'
  );
  assert.equal(
    fixture.status_page_admission_reconciliation.current_owner.identity,
    'public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb)'
  );
  assert.equal(fixture.status_page_admission_reconciliation.current_owner.statement_timeout_ms, 5000);
  assert.equal(
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.output_schema_changed,
    false
  );
  assert.equal(
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.new_ui_action_added,
    false
  );
  assert.equal(fixture.status_page_admission_reconciliation.deterministic_negative_cells.length, 13);
  assert.equal(fixture.status_page_admission_reconciliation.deterministic_positive_cells.length, 6);
  assert.equal(
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.provisional_local_owner_source_sha256,
    '86fb2a3f8bafb2bb0669026d49c51e0a488cd2e8884751baddf114c7134e0195'
  );
  assert.equal(
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.transient_reviewed_source_sha256_rejected_after_disk_drift,
    '940128517fecbb68a8073062d64dab8f1494ffdcbaf7562ac50f76ac0f285de3'
  );
  assert.equal(
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.source_manifest_status,
    'FINAL_SOURCE_MANIFEST_REFROZEN'
  );
  assert.equal(
    fixture.status_page_admission_reconciliation.relevant_boolean_and_action_outputs.required_unchanged.length,
    17
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.classification,
    'CURRENT_CANONICAL_PRODUCER_IDENTITY_OMISSION'
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.policy_or_economic_defect,
    false
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.current_owner.source_sha256,
    '28069c242c44e77333c15d599cbac1526ba966af274aad00ab2afda55018f01a'
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.candidate_owner.path,
    MANUAL_CARRY_FORWARD_IDENTITY_PATH
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.candidate_owner.source_sha256,
    MANUAL_CARRY_FORWARD_IDENTITY_SHA256
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.candidate_owner.amount_sign_tax_vat_channel_payee_or_eligibility_changed,
    false
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.all_terminal_current,
    true
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.source_preview_missing_or_changed_amount_channel_count,
    0
  );
  assert.equal(
    fixture.manual_carry_forward_economic_identity_reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.pg18_and_complete_candidate_matrix_pending,
    true
  );
  assert.equal(fixture.candidate_green_contract.candidate_scope_contract_version, 2);
  assert.equal(fixture.candidate_green_contract.candidate_scope_hash_version, 2);
  assert.equal(fixture.candidate_green_contract.source_row_count_semantics, 'FINANCIAL_ONLY');
  assert.equal(fixture.candidate_green_contract.communication_cleanup_contract_version, 2);
  assert.equal(fixture.schema_unreachable_detector_guards.length, 3);
  assert.equal(fixture.candidate_green_contract.provider_attempt_delta, 0);
  assert.equal(fixture.candidate_green_contract.settlement_delta, 0);
  assert.equal(fixture.candidate_green_contract.remittance_delta, 0);
  assert.equal(fixture.candidate_green_contract.prepare_expand_apply_statement_timeout_ms, 6000);
  assert.equal(fixture.candidate_green_contract.prepare_expand_apply_lock_timeout_ms, 1000);
  assert.equal(fixture.candidate_green_contract.integrity_checker_statement_timeout_ms, 5000);
  assert.equal(fixture.candidate_green_contract.outer_harness_statement_timeout_ms, 15000);
  assert.equal(fixture.candidate_green_contract.outer_harness_lock_timeout_ms, 1500);
  assert.equal(fixture.candidate_green_contract.outer_harness_idle_in_transaction_timeout_ms, 30000);
  assert.deepEqual(
    fixture.established_current_source_prerequisites.owner_paths,
    [
      'supabase/repeatable/04092026_2118_banking_pay_multi_candidate_cancel_continuation_v1.sql',
      'supabase/repeatable/05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql',
      'supabase/repeatable/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql',
      'supabase/migrations/05092026_1330_banking_pay_bank_event_movement_classification_v1.sql',
      'supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql',
      'supabase/repeatable/04092026_2350_banking_pay_cancellation_completion_v1.sql'
    ]
  );
  assert.equal(fixture.established_current_source_prerequisites.applied_identically_in_red_and_green, true);
  assert.deepEqual(fixture.candidate_install_contract.fresh_assignment_owner_paths, [
    FRESH_COMMUNICATION_PREPARE_PATH,
    FRESH_COMMUNICATION_EXPAND_PATH
  ]);
  assert.equal(
    fixture.candidate_install_contract.required_manual_carry_forward_identity_owner_path,
    MANUAL_CARRY_FORWARD_IDENTITY_PATH
  );
  assert.deepEqual(fixture.candidate_install_contract.superseded_never_created_paths, [
    'supabase/repeatable/07092026_2011_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql',
    'supabase/repeatable/07092026_2012_banking_pay_unpaid_cancellation_communication_v2_expand_v1.sql'
  ]);
  return fixture;
}

function expandLogicalCases(fixture) {
  const cases = [];
  for (const engine of fixture.matrix.axes.engines) {
    for (const route of fixture.matrix.axes.routes) {
      for (const scope of fixture.matrix.axes.scopes) {
        for (const reason of fixture.reason_cases) {
          cases.push({
            case_id: `${engine}_${route}_${scope}_${reason.reason}`,
            engine,
            route,
            scope,
            reason: reason.reason,
            batch_channel: reason.batch_channel,
            bad_item_channel: reason.bad_item_channel
          });
        }
      }
    }
  }
  assert.equal(cases.length, fixture.matrix.logical_case_count);
  assert.equal(new Set(cases.map((row) => row.case_id)).size, cases.length);
  return cases;
}

function buildPhysicalRuns(logicalCases) {
  const grouped = new Map();
  for (const logicalCase of logicalCases) {
    const key = [
      logicalCase.engine,
      logicalCase.route,
      logicalCase.scope,
      logicalCase.batch_channel
    ].join(':');
    const current = grouped.get(key) || {
      run_id: key.replaceAll(':', '_'),
      engine: logicalCase.engine,
      route: logicalCase.route,
      scope: logicalCase.scope,
      batch_channel: logicalCase.batch_channel,
      logical_case_ids: [],
      reasons: []
    };
    current.logical_case_ids.push(logicalCase.case_id);
    current.reasons.push(logicalCase.reason);
    grouped.set(key, current);
  }
  const physicalRuns = [...grouped.values()];
  assert.equal(physicalRuns.length, 16);
  assert.ok(physicalRuns.every((row) => (
    row.batch_channel === 'PAYE'
      ? row.logical_case_ids.length === 3
      : row.logical_case_ids.length === 2
  )));
  return physicalRuns;
}

function normalizedFilesystemPath(value) {
  const resolved = path.resolve(value);
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function assertSafeResultDestination(resolved) {
  const relative = path.relative(repoRoot, resolved);
  assert.ok(relative && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
  const rootReal = fs.realpathSync.native(repoRoot);
  const parts = relative.split(path.sep).filter(Boolean);
  let lexical = repoRoot;
  let expectedReal = rootReal;
  for (let index = 0; index < parts.length; index += 1) {
    lexical = path.join(lexical, parts[index]);
    expectedReal = path.join(expectedReal, parts[index]);
    const isLeaf = index === parts.length - 1;
    let status = null;
    try {
      status = fs.lstatSync(lexical);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
    if (!status) {
      assert.equal(isLeaf, true, `H12 result parent is missing: ${lexical}`);
      continue;
    }
    assert.equal(
      status.isSymbolicLink(),
      false,
      `H12_RESULT_DESTINATION_REPARSE_POINT_REJECTED: ${lexical}`
    );
    assert.equal(
      normalizedFilesystemPath(fs.realpathSync.native(lexical)),
      normalizedFilesystemPath(expectedReal),
      `H12_RESULT_DESTINATION_REALPATH_ESCAPE_REJECTED: ${lexical}`
    );
    if (isLeaf) {
      assert.equal(status.isFile(), true, `H12 result destination is not a regular file: ${lexical}`);
    } else {
      assert.equal(status.isDirectory(), true, `H12 result parent is not a directory: ${lexical}`);
    }
  }
  return resolved;
}

function resolveSafeResultPath(selectedEngineNames) {
  const enginesKey = [...selectedEngineNames].sort().join('_');
  assert.match(enginesKey, /^(?:PG17|PG18|PG17_PG18)$/);
  const defaultName = SNAPSHOT_EQUIVALENCE_ONLY
    ? `P12_SOURCE_LESS_CANCELLATION_SNAPSHOT_EQUIVALENCE_${enginesKey}_V1.json`
    : `P12_SOURCE_LESS_CANCELLATION_RUNTIME_RESULTS_${enginesKey}_V1.json`;
  const requested = String(process.env.H12_SOURCE_LESS_RESULT_PATH || '').trim();
  const resolved = path.resolve(requested || path.join(resultDirectory, defaultName));
  assert.equal(
    path.dirname(resolved).toLowerCase(),
    path.resolve(resultDirectory).toLowerCase(),
    'H12 result path must stay directly inside codex_outputs/h12-banking-draft-v8'
  );
  assert.match(
    path.basename(resolved),
    /^P12_SOURCE_LESS_CANCELLATION_(?:RUNTIME_RESULTS|SNAPSHOT_EQUIVALENCE)_(?:PG17|PG18|PG17_PG18)(?:_[A-Z0-9_]+)?_V1\.json$/,
    'H12 result filename is outside the bounded evidence contract'
  );
  for (const engineName of selectedEngineNames) {
    assert.ok(path.basename(resolved).includes(engineName), `H12 result path omits ${engineName}`);
  }
  return assertSafeResultDestination(resolved);
}

function selectedFilter(value) {
  return new Set(
    String(value || '')
      .split(',')
      .map((part) => part.trim().toUpperCase())
      .filter(Boolean)
  );
}

function filterPhysicalRuns(runs) {
  const engineFilter = selectedFilter(process.env.H12_SOURCE_LESS_ENGINE);
  const routeFilter = selectedFilter(process.env.H12_SOURCE_LESS_ROUTE);
  const scopeFilter = selectedFilter(process.env.H12_SOURCE_LESS_SCOPE);
  const channelFilter = selectedFilter(process.env.H12_SOURCE_LESS_CHANNEL);
  return runs.filter((row) =>
    (!engineFilter.size || engineFilter.has(row.engine))
    && (!routeFilter.size || routeFilter.has(row.route))
    && (!scopeFilter.size || scopeFilter.has(row.scope))
    && (!channelFilter.size || channelFilter.has(row.batch_channel))
  );
}

function candidateSourcePaths() {
  const raw = String(process.env.H12_SOURCE_LESS_CANDIDATE_SQL_PATHS || '').trim();
  if (MODE === 'CURRENT_RED') {
    assert.equal(raw, '', 'current-red mode must not install candidate SQL');
    return [];
  }
  assert.equal(MODE, 'CANDIDATE_GREEN', `unsupported H12_SOURCE_LESS_MODE ${MODE}`);
  assert.ok(raw, 'candidate-green mode requires H12_SOURCE_LESS_CANDIDATE_SQL_PATHS');
  const resolved = raw.split(';').map((entry) => entry.trim()).filter(Boolean).map((entry) => {
    const relative = entry.replaceAll('\\', '/');
    assert.match(relative, /^supabase\/(?:migrations|repeatable)\/[0-9]{8}_[0-9]{4}_[a-z0-9_]+\.sql$/);
    const absolute = path.resolve(repoRoot, ...relative.split('/'));
    assert.ok(
      absolute.startsWith(path.join(repoRoot, 'supabase', 'migrations') + path.sep)
      || absolute.startsWith(path.join(repoRoot, 'supabase', 'repeatable') + path.sep)
    );
    assert.equal(fs.existsSync(absolute), true, `candidate SQL missing: ${relative}`);
    return absolute;
  });
  assert.equal(new Set(resolved).size, resolved.length, 'candidate SQL paths must be unique');
  const relativePaths = resolved.map((absolute) => path.relative(repoRoot, absolute).replaceAll('\\', '/'));
  assert.ok(relativePaths.includes(FRESH_COMMUNICATION_PREPARE_PATH), 'fresh communication-V2 prepare owner missing');
  assert.ok(relativePaths.includes(FRESH_COMMUNICATION_EXPAND_PATH), 'fresh communication-V2 expand owner missing');
  assert.ok(relativePaths.includes(VERSIONED_INTEGRITY_CHECKER_PATH), 'versioned integrity checker owner missing');
  assert.ok(relativePaths.includes(SOURCE_LESS_APPLY_PATH), 'source-less apply owner missing');
  assert.ok(relativePaths.includes(STATUS_PAGE_ADMISSION_PATH), 'status-page cancellation admission owner missing');
  assert.ok(relativePaths.includes(MANUAL_CARRY_FORWARD_IDENTITY_PATH), 'manual carry-forward identity owner missing');
  const cohortIndex = relativePaths.indexOf(CANDIDATE_DIRTY_COHORT_PATH);
  const invalidatorIndex = relativePaths.indexOf(SCOPE_INVALIDATOR_PAIR_ARRAYS_PATH);
  const frozenScopeIndex = relativePaths.indexOf(CANCEL_RETURN_FROZEN_SCOPE_INDEX_PATH);
  const selectionIntentIndex = relativePaths.indexOf(CANCEL_RETURN_SELECTION_INTENT_PATH);
  const finalSelectionCountIndex = relativePaths.indexOf(CERTIFIED_PREVIEW_FINAL_SELECTION_COUNT_PATH);
  const noMoneyWorkbenchReturnIndex = relativePaths.indexOf(NO_MONEY_WORKBENCH_RETURN_PATH);
  assert.ok(cohortIndex >= 0, 'candidate dirty-cohort owner missing');
  assert.equal(
    invalidatorIndex,
    cohortIndex + 1,
    'scope invalidator replacement must immediately follow the dirty-cohort owner'
  );
  assert.ok(frozenScopeIndex >= 0, 'cancel-return frozen-scope index missing');
  assert.equal(selectionIntentIndex, invalidatorIndex + 1, 'cancel-return selection-intent owner must follow scope invalidator');
  assert.equal(finalSelectionCountIndex, selectionIntentIndex + 1, 'certified-preview final-selection-count owner must follow selection intent');
  assert.equal(noMoneyWorkbenchReturnIndex, finalSelectionCountIndex + 1, 'no-money Workbench-return owner must follow final-selection-count owner');
  assert.equal(noMoneyWorkbenchReturnIndex, relativePaths.length - 1, 'no-money Workbench-return owner must be the final candidate source');
  const invalidatorSource = fs.readFileSync(resolved[invalidatorIndex], 'utf8').replaceAll('\r\n', '\n');
  const invalidatorIncludes = [...invalidatorSource.matchAll(/^\s*\\ir\s+([^\s]+)\s*$/gm)]
    .map((match) => match[1]);
  assert.deepEqual(
    invalidatorIncludes,
    [path.basename(SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH)],
    'scope invalidator replacement must have one exact historical include'
  );
  const invalidatorHistoricalAbsolute = path.resolve(
    repoRoot,
    ...SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH.split('/')
  );
  assert.equal(
    fs.existsSync(invalidatorHistoricalAbsolute),
    true,
    'scope invalidator historical include is missing'
  );
  assert.equal(
    sha256(fs.readFileSync(invalidatorHistoricalAbsolute)),
    SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_SHA256,
    'scope invalidator historical include hash changed'
  );
  const sourceLessApplyAbsolute = resolved[relativePaths.indexOf(SOURCE_LESS_APPLY_PATH)];
  const sourceLessApplySource = fs.readFileSync(sourceLessApplyAbsolute, 'utf8').replaceAll('\r\n', '\n');
  const noMoneyDefinitionStart = sourceLessApplySource.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_no_money_unwind_apply_work_item('
  );
  const noMoneyDefinitionEnd = sourceLessApplySource.indexOf(
    '\nALTER FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid)',
    noMoneyDefinitionStart
  );
  assert.ok(
    noMoneyDefinitionStart >= 0 && noMoneyDefinitionEnd > noMoneyDefinitionStart,
    'source-less apply owner has no exact no-money definition boundary'
  );
  const noMoneyDefinition = sourceLessApplySource.slice(noMoneyDefinitionStart, noMoneyDefinitionEnd);
  const noMoneyResultStart = noMoneyDefinition.indexOf('  v_result := jsonb_build_object(');
  const noMoneyResultEnd = noMoneyDefinition.indexOf(
    '\n\n  UPDATE public.pay_payment_correction_work_items AS applied_work_item',
    noMoneyResultStart
  );
  assert.ok(
    noMoneyResultStart >= 0 && noMoneyResultEnd > noMoneyResultStart,
    'source-less apply owner has no exact result-object boundary'
  );
  const noMoneyResultExpression = noMoneyDefinition.slice(noMoneyResultStart, noMoneyResultEnd);
  const noMoneyResultPieces = noMoneyResultExpression.split(/\n  \) \|\| jsonb_build_object\(\n/);
  const noMoneyResultPairCounts = noMoneyResultPieces.map((piece) => (
    [...piece.matchAll(/^    '([^']+)',/gm)].length
  ));
  assert.deepEqual(
    noMoneyResultPairCounts,
    [43, 11, 20, 8],
    'source-less apply result must retain four bounded PostgreSQL constructor calls'
  );
  const noMoneyResultKeys = noMoneyResultPieces.flatMap((piece) => (
    [...piece.matchAll(/^    '([^']+)',/gm)].map((match) => match[1])
  ));
  assert.equal(noMoneyResultKeys.length, 82, 'source-less apply result field count changed');
  assert.equal(new Set(noMoneyResultKeys).size, 82, 'source-less apply result contains duplicate fields');
  assert.equal(
    sha256([...noMoneyResultKeys].sort().join('\n')),
    SOURCE_LESS_NO_MONEY_RESULT_KEYSET_SHA256,
    'source-less apply result key set changed'
  );
  const noMoneyResultAuditStart = noMoneyDefinition.indexOf(
    "    'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT',",
    noMoneyResultEnd
  );
  const noMoneyResultReturnStart = noMoneyDefinition.indexOf(
    '\n  RETURN v_result;',
    noMoneyResultAuditStart
  );
  assert.ok(
    noMoneyResultAuditStart > noMoneyResultEnd
      && noMoneyResultReturnStart > noMoneyResultAuditStart,
    'source-less apply result has no exact audit-to-return binding'
  );
  assert.match(
    noMoneyDefinition.slice(noMoneyResultAuditStart, noMoneyResultReturnStart),
    /'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT',\s*\n\s*v_result,/,
    'source-less apply result capture must receive the exact returned v_result'
  );
  sourceLessNoMoneyResultKeys = Object.freeze([...noMoneyResultKeys]);
  assert.equal(
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.source_manifest_status,
    'FINAL_SOURCE_MANIFEST_REFROZEN',
    'candidate execution prohibited until the complete source manifest, including the conditional follow-up owner, is refrozen'
  );
  const statusAdmissionAbsolute = resolved[relativePaths.indexOf(STATUS_PAGE_ADMISSION_PATH)];
  assert.equal(
    sha256(fs.readFileSync(statusAdmissionAbsolute)),
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.provisional_local_owner_source_sha256,
    'status-page owner differs from the final frozen source manifest'
  );
  const manualCarryForwardIdentityAbsolute = resolved[relativePaths.indexOf(MANUAL_CARRY_FORWARD_IDENTITY_PATH)];
  const manualCarryForwardIdentitySource = fs.readFileSync(manualCarryForwardIdentityAbsolute, 'utf8').replaceAll('\r\n', '\n');
  assert.equal(
    sha256(fs.readFileSync(manualCarryForwardIdentityAbsolute)),
    MANUAL_CARRY_FORWARD_IDENTITY_SHA256,
    'manual carry-forward identity owner differs from its reviewed source hash'
  );
  assert.equal(
    [...manualCarryForwardIdentitySource.matchAll(/create\s+(?:or\s+replace\s+)?function\s+public\.pay_preview_candidate_build_canonical_lines\s*\(/gi)].length,
    1,
    'manual carry-forward identity owner must contain exactly one canonical producer definition'
  );
  assert.match(manualCarryForwardIdentitySource, /'component_key_type', 'MANUAL_CARRY_FORWARD'/);
  assert.match(manualCarryForwardIdentitySource, /'key_type', 'MANUAL_CARRY_FORWARD'/);
  assert.match(manualCarryForwardIdentitySource, /'economic_key', jsonb_strip_nulls\(jsonb_build_object\(/);
  assert.ok(
    relativePaths.every((relative) => !/07092026_201[12]_banking_pay_unpaid_cancellation_communication_v2_/i.test(relative)),
    'superseded never-created 2011/2012 path must not be consumed'
  );
  const requiredApplyOwners = new Map([
    ['pay_pre_bank_cancel_apply_work_item', 0],
    ['pay_no_money_unwind_apply_work_item', 0]
  ]);
  const requiredFreshAssignmentOwners = new Map([
    ['pay_payment_correction_selection_prepare_chunk_v1', 0],
    ['pay_payment_correction_expand_work', 0]
  ]);
  let integrityCheckerOwnerCount = 0;
  for (const absolute of resolved) {
    const source = fs.readFileSync(absolute, 'utf8').replaceAll('\r\n', '\n');
    const relative = path.relative(repoRoot, absolute).replaceAll('\\', '/');
    const integrityOwnerPattern = /create\s+(?:or\s+replace\s+)?function\s+public\.pay_payment_correction_integrity_check_v1\s*\(/gi;
    const integrityOwnerCount = [...source.matchAll(integrityOwnerPattern)].length;
    if (integrityOwnerCount > 0) {
      assert.equal(integrityOwnerCount, 1, 'integrity checker must have one candidate definition');
      assert.equal(relative, VERSIONED_INTEGRITY_CHECKER_PATH, 'integrity checker must use its exact additive owner path');
      assert.match(source, /candidate_scope_contract_version/);
      assert.match(source, /candidate_scope_hash_version/);
      assert.match(source, /communication_cleanup_contract_version/);
      assert.match(source, /FINANCIAL_AND_QUEUED_COMMUNICATIONS/);
      assert.match(source, /FINANCIAL_ONLY/);
      assert.match(source, /SELECTION_HASH_MISMATCH/);
      assert.match(source, /SET\s+statement_timeout\s+TO\s+'5000ms'/i);
      integrityCheckerOwnerCount += 1;
    }
    for (const ownerName of requiredFreshAssignmentOwners.keys()) {
      const ownerPattern = new RegExp(
        `create\\s+(?:or\\s+replace\\s+)?function\\s+public\\.${ownerName}\\s*\\(`,
        'gi'
      );
      const ownerCount = [...source.matchAll(ownerPattern)].length;
      if (ownerCount === 0) continue;
      assert.equal(ownerCount, 1, `${ownerName} must have one candidate definition`);
      requiredFreshAssignmentOwners.set(ownerName, requiredFreshAssignmentOwners.get(ownerName) + 1);
      const expectedPath = ownerName.endsWith('selection_prepare_chunk_v1')
        ? FRESH_COMMUNICATION_PREPARE_PATH
        : FRESH_COMMUNICATION_EXPAND_PATH;
      assert.equal(relative, expectedPath, `${ownerName} must use its reconciled generator-authentic owner path`);
      assert.match(source, /communication_cleanup_contract_version/);
      assert.match(source, /FINANCIAL_ONLY/);
      assert.match(source, /SET\s+statement_timeout\s+TO\s+'6000ms'/i);
      assert.match(source, /SET\s+lock_timeout\s+TO\s+'1000ms'/i);
      assert.match(
        source,
        /communication_cleanup_contract_version[\s\S]{0,240}(?:2|'2')/i,
        `${ownerName} must explicitly assign communication contract V2`
      );
    }
    for (const ownerName of requiredApplyOwners.keys()) {
      const ownerPattern = new RegExp(
        `create\\s+(?:or\\s+replace\\s+)?function\\s+public\\.${ownerName}\\s*\\(`,
        'gi'
      );
      const ownerCount = [...source.matchAll(ownerPattern)].length;
      if (ownerCount === 0) continue;
      assert.equal(ownerCount, 1, `${ownerName} must have one candidate definition`);
      requiredApplyOwners.set(ownerName, requiredApplyOwners.get(ownerName) + 1);
      // The complete replacement must preserve persisted V1 and V2/c1 replay,
      // including their historical mail behavior.  Therefore mail references
      // are expected in the legacy branch; only an explicitly marked fresh
      // communication-contract V2 request may bypass them.
      assert.match(source, /communication_cleanup_contract_version/i);
      assert.match(source, /FINANCIAL_AND_QUEUED_COMMUNICATIONS/);
      assert.match(source, /FINANCIAL_ONLY/);
      assert.match(source, /SET\s+statement_timeout\s+TO\s+'6000ms'/i);
      assert.match(source, /SET\s+lock_timeout\s+TO\s+'1000ms'/i);
      assert.match(source, /\bmail_outbox\b/i, `${ownerName} must retain legacy mail replay`);
      assert.match(
        source,
        /_pay_payment_correction_mail_scope_match/i,
        `${ownerName} must retain the historical V1\/V2-c1 mail matcher`
      );
      assert.match(
        source,
        /communication_cleanup_contract_version[\s\S]{0,240}(?:=|IS\s+NOT\s+DISTINCT\s+FROM)\s*2/i,
        `${ownerName} must have an explicit communication-contract V2 path`
      );
    }
  }
  for (const [ownerName, ownerCount] of requiredFreshAssignmentOwners) {
    assert.equal(ownerCount, 1, `${ownerName} fresh assignment definition missing or duplicated`);
  }
  for (const [ownerName, ownerCount] of requiredApplyOwners) {
    assert.equal(ownerCount, 1, `${ownerName} candidate definition missing or duplicated`);
  }
  assert.equal(integrityCheckerOwnerCount, 1, 'integrity checker candidate definition missing or duplicated');
  const priority = (absolute) => {
    const relative = path.relative(repoRoot, absolute).replaceAll('\\', '/');
    if (relative.startsWith('supabase/migrations/')) return 0;
    if (relative.includes('_sourceless_admission_')) return 10;
    if (relative === FRESH_COMMUNICATION_PREPARE_PATH) return 20;
    if (relative === FRESH_COMMUNICATION_EXPAND_PATH) return 30;
    if (relative === VERSIONED_INTEGRITY_CHECKER_PATH) return 35;
    if (relative.includes('_sourceless_apply_')) return 40;
    if (relative.includes('_investigation_alert_')) return 50;
    if (relative === STATUS_PAGE_ADMISSION_PATH) return 55;
    if (relative === MANUAL_CARRY_FORWARD_IDENTITY_PATH) return 60;
    if (relative === CANDIDATE_DIRTY_COHORT_PATH) return 65;
    if (relative === SCOPE_INVALIDATOR_PAIR_ARRAYS_PATH) return 70;
    if (relative === CANCEL_RETURN_SELECTION_INTENT_PATH) return 75;
    if (relative === CERTIFIED_PREVIEW_FINAL_SELECTION_COUNT_PATH) return 80;
    if (relative === NO_MONEY_WORKBENCH_RETURN_PATH) return 85;
    return 60;
  };
  return [...resolved].sort((left, right) => priority(left) - priority(right));
}

function prepareTemplateDatabase(engine) {
  assert.match(engine.templateDatabase, /^(?:h12_sourceless_baseline|h12_rg5_builder)_pg(?:17|18)$/);
  const dumpPath = `/tmp/${engine.templateDatabase}.dump`;
  docker(engine.container, ['dropdb', '-U', 'postgres', '--if-exists', engine.templateDatabase]);
  docker(engine.container, ['rm', '-f', dumpPath]);
  try {
    docker(engine.container, ['pg_dump', '-U', 'postgres', '-d', SOURCE_DATABASE, '-Fc', '-f', dumpPath]);
    docker(engine.container, ['createdb', '-U', 'postgres', engine.templateDatabase]);
    docker(engine.container, [
      'pg_restore', '-U', 'postgres', '-d', engine.templateDatabase,
      '--no-owner', '--exit-on-error', dumpPath
    ]);
  } finally {
    docker(engine.container, ['rm', '-f', dumpPath], { allowFailure: true });
  }
}

function dropTemplateDatabase(engine) {
  assert.match(engine.templateDatabase, /^(?:h12_sourceless_baseline|h12_rg5_builder)_pg(?:17|18)$/);
  docker(
    engine.container,
    ['dropdb', '-U', 'postgres', '--if-exists', engine.templateDatabase],
    { allowFailure: true }
  );
}

function assertPreparedTemplateDatabase(engine) {
  assert.match(engine.preparedTemplateDatabase, /^h12_rg5_prepared_pg(?:17|18)$/);
  assert.notEqual(engine.preparedTemplateDatabase, engine.templateDatabase);
  assert.notEqual(engine.preparedTemplateDatabase, engine.targetDatabase);
}

function dropPreparedTemplateDatabase(engine) {
  assertPreparedTemplateDatabase(engine);
  docker(
    engine.container,
    ['dropdb', '-U', 'postgres', '--if-exists', engine.preparedTemplateDatabase],
    { allowFailure: true }
  );
}

function dropTaskOwnedTargetDatabase(engine) {
  assert.match(engine.targetDatabase, /^(?:h2_cancel_v8|h12_rg5_builder_cancel)_pg(?:17|18)$/);
  docker(
    engine.container,
    ['dropdb', '-U', 'postgres', '--if-exists', engine.targetDatabase],
    { allowFailure: true }
  );
}

function promoteFreshBoundaryToPreparedTemplate(engine) {
  assertPreparedTemplateDatabase(engine);
  assert.match(engine.targetDatabase, /^(?:h2_cancel_v8|h12_rg5_builder_cancel)_pg(?:17|18)$/);
  docker(engine.container, [
    'psql', '-U', 'postgres', '-d', 'postgres', '-X', '-v', 'ON_ERROR_STOP=1', '-c',
    `ALTER DATABASE ${engine.targetDatabase} RENAME TO ${engine.preparedTemplateDatabase}`
  ]);
}

function runSnapshotControl(engine, transformedRunnerPath, envOverrides) {
  const executed = spawnSync(process.execPath, [transformedRunnerPath], {
    cwd: repoRoot,
    env: {
      ...process.env,
      ...ESTABLISHED_CURRENT_BASELINE_FLAGS,
      H2_CANCEL_TARGET: engine.target,
      H2_CANCEL_CHANNEL: '',
      H2_CANCEL_SCOPE: 'ONE_CANDIDATE',
      H2_CANCEL_PAYMENT_STATE: 'DRAFT',
      H2_CANCEL_PRODUCTION_SHAPED_SOURCE: 'true',
      H2_CANCEL_ESTABLISH_CURRENT_SOURCE: 'true',
      H2_CANCEL_DRAIN_WORKBENCH: 'false',
      H2_CANCEL_SIMULATE_RESPONSE_LOSS: 'false',
      H2_CANCEL_INSTRUMENT_REFRESH: 'false',
      H2_CANCEL_INSTRUMENT_PROCESS_ROUTE: 'false',
      H2_CANCEL_COMPACT_OUTPUT: 'false',
      H12_SOURCE_LESS_FIXTURE_MODE: 'FULL',
      H12_SOURCE_LESS_PERSISTED_CONTRACT: '',
      H12_SOURCE_LESS_DEFER_CANDIDATE_INSTALL: 'false',
      ...envOverrides
    },
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true
  });
  assert.equal(executed.status, 0, `${executed.stdout}\n${executed.stderr}`);
  return parseRunnerOutput(executed.stdout);
}

function prepareAndVerifyH12Snapshot(engineName, engine, transformedRunnerPath) {
  assertPreparedTemplateDatabase(engine);
  dropPreparedTemplateDatabase(engine);
  const startedAt = performance.now();
  const legacyFresh = runSnapshotControl(engine, transformedRunnerPath, {
    H2_CANCEL_KEEP_DATABASE: 'true',
    H12_SOURCE_LESS_PREPARE_SNAPSHOT: 'true',
    H12_SOURCE_LESS_CAPTURE_PRE_CANCELLATION_BOUNDARY: 'true',
    H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT: 'false',
    H12_SOURCE_LESS_VERIFY_PREPARED_SNAPSHOT: 'false'
  });
  const legacyBoundary = legacyFresh.results?._h12_prepared_snapshot_boundary;
  assert.ok(legacyBoundary, `${engineName} legacy-fresh snapshot boundary missing`);
  assert.equal(legacyBoundary.legacy_fresh_path_used, true);
  assert.equal(legacyBoundary.route_mutation_started, false);
  assert.equal(legacyBoundary.fixture_absent, true);
  promoteFreshBoundaryToPreparedTemplate(engine);

  const preparedClone = runSnapshotControl(engine, transformedRunnerPath, {
    H2_CANCEL_KEEP_DATABASE: 'false',
    H12_SOURCE_LESS_PREPARE_SNAPSHOT: 'false',
    H12_SOURCE_LESS_CAPTURE_PRE_CANCELLATION_BOUNDARY: 'false',
    H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT: 'false',
    H12_SOURCE_LESS_VERIFY_PREPARED_SNAPSHOT: 'true',
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SOURCE: legacyBoundary.source_fingerprint,
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SEMANTIC_SHA256: legacyBoundary.semantic_sha256,
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_EXACT_SHA256: legacyBoundary.exact_sha256
  });
  const cloneBoundary = preparedClone.results?._h12_prepared_snapshot_clone;
  assert.ok(cloneBoundary, `${engineName} prepared clone snapshot boundary missing`);
  assert.equal(cloneBoundary.source_fingerprint, legacyBoundary.source_fingerprint);
  assert.equal(cloneBoundary.semantic_sha256, legacyBoundary.semantic_sha256);
  assert.equal(cloneBoundary.exact_sha256, legacyBoundary.exact_sha256);
  assert.deepEqual(cloneBoundary.semantic_manifest, legacyBoundary.semantic_manifest);
  assert.deepEqual(cloneBoundary.exact_manifest, legacyBoundary.exact_manifest);
  return {
    contract: 'H12_LEGACY_FRESH_PREPARED_CLONE_EQUIVALENCE_V1',
    engine: engineName,
    prepared_template_database: engine.preparedTemplateDatabase,
    source_fingerprint: legacyBoundary.source_fingerprint,
    semantic_sha256: legacyBoundary.semantic_sha256,
    exact_sha256: legacyBoundary.exact_sha256,
    relation_count: legacyBoundary.relation_count,
    sequence_count: legacyBoundary.sequence_count,
    distinct_source_session_count: legacyBoundary.distinct_source_session_count,
    one_anchor_per_distinct_source_session: true,
    legacy_fresh_to_prepared_clone_semantic_equal: true,
    legacy_fresh_to_prepared_clone_exact_equal: true,
    route_or_fixture_mutation_before_snapshot: false,
    elapsed_ms: Number((performance.now() - startedAt).toFixed(3))
  };
}

function verifyExistingH12Snapshot(engineName, engine, transformedRunnerPath) {
  assertPreparedTemplateDatabase(engine);
  const expectedSource = String(
    process.env.H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SOURCE || ''
  ).trim();
  const expectedSemantic = String(
    process.env.H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SEMANTIC_SHA256 || ''
  ).trim();
  const expectedExact = String(
    process.env.H12_SOURCE_LESS_EXPECTED_SNAPSHOT_EXACT_SHA256 || ''
  ).trim();
  assert.match(expectedSource, /^[0-9a-f]{64}$/i, `${engineName} expected snapshot source missing`);
  assert.match(expectedSemantic, /^[0-9a-f]{64}$/i, `${engineName} expected snapshot semantic hash missing`);
  assert.match(expectedExact, /^[0-9a-f]{64}$/i, `${engineName} expected snapshot exact hash missing`);
  const startedAt = performance.now();
  const preparedClone = runSnapshotControl(engine, transformedRunnerPath, {
    H2_CANCEL_KEEP_DATABASE: 'false',
    H12_SOURCE_LESS_PREPARE_SNAPSHOT: 'false',
    H12_SOURCE_LESS_CAPTURE_PRE_CANCELLATION_BOUNDARY: 'false',
    H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT: 'false',
    H12_SOURCE_LESS_VERIFY_PREPARED_SNAPSHOT: 'true',
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SOURCE: expectedSource,
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SEMANTIC_SHA256: expectedSemantic,
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_EXACT_SHA256: expectedExact
  });
  const cloneBoundary = preparedClone.results?._h12_prepared_snapshot_clone;
  assert.ok(cloneBoundary, `${engineName} existing prepared snapshot boundary missing`);
  assert.equal(cloneBoundary.source_fingerprint, expectedSource);
  assert.equal(cloneBoundary.semantic_sha256, expectedSemantic);
  assert.equal(cloneBoundary.exact_sha256, expectedExact);
  return {
    contract: 'H12_EXISTING_PREPARED_CLONE_EXACT_IDENTITY_V1',
    engine: engineName,
    prepared_template_database: engine.preparedTemplateDatabase,
    source_fingerprint: expectedSource,
    semantic_sha256: expectedSemantic,
    exact_sha256: expectedExact,
    relation_count: cloneBoundary.relation_count,
    sequence_count: cloneBoundary.sequence_count,
    distinct_source_session_count: cloneBoundary.distinct_source_session_count,
    one_anchor_per_distinct_source_session: true,
    legacy_fresh_to_prepared_clone_semantic_equal: true,
    legacy_fresh_to_prepared_clone_exact_equal: true,
    route_or_fixture_mutation_before_snapshot: false,
    resumed_from_previously_verified_snapshot: true,
    elapsed_ms: Number((performance.now() - startedAt).toFixed(3))
  };
}

// These functions are stringified into a private temporary copy of the
// established cancellation runner.  They deliberately depend only on globals
// already defined by that runner plus the constants injected alongside them.
function installH12CandidateSources(target) {
  for (const candidatePath of H12_CANDIDATE_SOURCE_PATHS) {
    if (candidatePath === H12_SCOPE_INVALIDATOR_PAIR_ARRAYS_PATH) {
      const created = runDocker(target.container, [
        'mktemp', '-d', '/tmp/h12-source-less-scope-invalidator-XXXXXX'
      ]);
      const includeDirectory = String(created.stdout || '').trim();
      assert.match(
        includeDirectory,
        /^\/tmp\/h12-source-less-scope-invalidator-[A-Za-z0-9]+$/,
        'unexpected scope invalidator include directory'
      );
      const candidateContainerPath = `${includeDirectory}/${path.basename(candidatePath)}`;
      const historicalContainerPath = `${includeDirectory}/${path.basename(H12_SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH)}`;
      try {
        for (const [sourcePath, containerPath] of [
          [candidatePath, candidateContainerPath],
          [H12_SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH, historicalContainerPath]
        ]) {
          const copied = spawnSync('docker', ['cp', sourcePath, `${target.container}:${containerPath}`], {
            encoding: 'utf8',
            windowsHide: true
          });
          if (copied.status !== 0) {
            throw new Error([copied.stdout, copied.stderr].filter(Boolean).join('\n'));
          }
        }
        runDocker(target.container, [
          'env',
          'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
          'psql', '-U', 'postgres', '-d', target.database,
          '-X', '-v', 'ON_ERROR_STOP=1', '-f', candidateContainerPath
        ]);
      } finally {
        runDocker(target.container, ['rm', '-rf', includeDirectory], { allowFailure: true });
      }
      continue;
    }
    const containerPath = `/tmp/${path.basename(candidatePath)}`;
    const copied = spawnSync('docker', ['cp', candidatePath, `${target.container}:${containerPath}`], {
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
}

function installH12Fixture(target) {
  const channel = String(process.env.H2_CANCEL_CHANNEL || '').trim().toUpperCase();
  assert.ok(channel === 'PAYE' || channel === 'UMBRELLA');
  const batchId = target.batches[channel];
  const fixtureMode = String(process.env.H12_SOURCE_LESS_FIXTURE_MODE || 'FULL').trim().toUpperCase();
  assert.ok(fixtureMode === 'FULL' || fixtureMode === 'MAIL_ONLY');
  const containerPath = `/tmp/${path.basename(H12_SETUP_SQL_PATH)}`;
  const copied = spawnSync('docker', ['cp', H12_SETUP_SQL_PATH, `${target.container}:${containerPath}`], {
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
      '-X', '-v', 'ON_ERROR_STOP=1', '-Atq',
      '-v', `h12_batch_id=${batchId}`,
      '-v', `h12_batch_channel=${channel}`,
      '-v', `h12_actor_id=${ACTOR_ID}`,
      '-v', `h12_fixture_mode=${fixtureMode}`,
      '-f', containerPath
    ]);
  } finally {
    runDocker(target.container, ['rm', '-f', containerPath], { allowFailure: true });
  }
}

function establishH12ProductionShapedWorkbenchSource(target, batchId) {
  const siblingBatches = queryJson(target, `
    WITH source_session AS (
      SELECT batch_row.source_workbench_session_id AS session_id
      FROM public.pay_batches AS batch_row
      WHERE batch_row.id = '${batchId}'::uuid
    )
    SELECT pg_catalog.jsonb_build_object(
      'batch_ids', COALESCE(
        pg_catalog.jsonb_agg(batch_row.id ORDER BY batch_row.id),
        '[]'::jsonb
      )
    )::text
    FROM public.pay_batches AS batch_row
    JOIN source_session
      ON source_session.session_id = batch_row.source_workbench_session_id
  `).value;
  assert.ok(Array.isArray(siblingBatches.batch_ids) && siblingBatches.batch_ids.length > 0);
  assert.ok(siblingBatches.batch_ids.includes(batchId));
  for (const siblingBatchId of siblingBatches.batch_ids) {
    establishProductionShapedWorkbenchSource(target, siblingBatchId);
  }
}

function establishH12CurrentWorkbenchAuthority(target, batchId) {
  const siblingBatches = queryJson(target, `
    WITH source_session AS (
      SELECT batch_row.source_workbench_session_id AS session_id
      FROM public.pay_batches AS batch_row
      WHERE batch_row.id = '${batchId}'::uuid
    )
    SELECT pg_catalog.jsonb_build_object(
      'batch_ids', COALESCE(
        pg_catalog.jsonb_agg(batch_row.id ORDER BY batch_row.id),
        '[]'::jsonb
      )
    )::text
    FROM public.pay_batches AS batch_row
    JOIN source_session
      ON source_session.session_id = batch_row.source_workbench_session_id
  `).value;
  assert.ok(Array.isArray(siblingBatches.batch_ids) && siblingBatches.batch_ids.length > 0);
  assert.ok(siblingBatches.batch_ids.includes(batchId));

  // establishH12ProductionShapedWorkbenchSource dirties every sibling batch in
  // the shared session.  Finish that legitimate transition for the complete
  // session before freezing history; otherwise a later sibling can still add
  // source-owned supersession bookkeeping after the snapshot was taken.
  let selectedBatchCurrentness = null;
  const completeSessionConvergence = [];
  for (const siblingBatchId of siblingBatches.batch_ids) {
    const convergence = drainCancellationWorkbenchSourceBuilds(target, siblingBatchId);
    completeSessionConvergence.push({
      batch_id: siblingBatchId,
      iteration_count: convergence.iteration_count,
      safe_step_count: convergence.safe_steps.length
    });
    if (siblingBatchId === batchId) selectedBatchCurrentness = convergence.currentness;
  }
  assert.ok(selectedBatchCurrentness);

  const historicalBefore = queryJson(target, `
    WITH source_session AS (
      SELECT batch_row.source_workbench_session_id AS session_id
      FROM public.pay_batches AS batch_row
      WHERE batch_row.id = '${batchId}'::uuid
    )
    SELECT pg_catalog.jsonb_build_object(
      'source_rows', COALESCE((
        SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(source_row) ORDER BY source_row.id)
        FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        JOIN source_session ON source_session.session_id = source_row.session_id
        WHERE source_row.status = 'SUPERSEDED'
      ), '[]'::jsonb),
      'preview_rows', COALESCE((
        SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(preview_row) ORDER BY preview_row.id)
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        JOIN source_session ON source_session.session_id = preview_row.session_id
        WHERE preview_row.status = 'SUPERSEDED'
      ), '[]'::jsonb)
    )::text
  `).value;
  assert.ok(historicalBefore.source_rows.length > 0, JSON.stringify(historicalBefore));
  assert.ok(historicalBefore.preview_rows.length > 0, JSON.stringify(historicalBefore));
  const sourceHistoryIds = historicalBefore.source_rows.map((row) => {
    assert.match(row.id, /^[0-9a-f-]{36}$/i);
    return `'${row.id}'::uuid`;
  }).join(',');
  const previewHistoryIds = historicalBefore.preview_rows.map((row) => {
    assert.match(row.id, /^[0-9a-f-]{36}$/i);
    return `'${row.id}'::uuid`;
  }).join(',');
  const sourceHistorySha256 = sha256(historicalBefore.source_rows);
  const previewHistorySha256 = sha256(historicalBefore.preview_rows);

  // Replay the complete session convergence after the historical snapshot.
  // Every sibling must already be quiescent: any iteration or processed repair
  // here would prove that the snapshot boundary was still moving.
  const quiescentSessionReplay = [];
  for (const siblingBatchId of siblingBatches.batch_ids) {
    const replay = drainCancellationWorkbenchSourceBuilds(target, siblingBatchId);
    assert.equal(replay.iteration_count, 0, JSON.stringify({ siblingBatchId, replay }));
    assert.equal(
      replay.safe_steps.some((step) => Number(step.processed || 0) !== 0),
      false,
      JSON.stringify({ siblingBatchId, replay })
    );
    quiescentSessionReplay.push({
      batch_id: siblingBatchId,
      iteration_count: replay.iteration_count,
      safe_step_count: replay.safe_steps.length,
      processed_count: 0
    });
    if (siblingBatchId === batchId) selectedBatchCurrentness = replay.currentness;
  }
  const after = queryJson(target, `
    WITH source_session AS (
      SELECT batch_row.source_workbench_session_id AS session_id
      FROM public.pay_batches AS batch_row
      WHERE batch_row.id = '${batchId}'::uuid
    ), session_candidates AS (
      SELECT scope_row.candidate_id
      FROM public.banking_pay_workbench_session_scope AS scope_row
      JOIN source_session ON source_session.session_id = scope_row.session_id
    ), duplicate_current_ordinals AS (
      SELECT
        source_row.candidate_id,
        source_row.source_publication_id,
        source_row.source_ordinal
      FROM public.banking_pay_workbench_candidate_source_lines AS source_row
      JOIN source_session ON source_session.session_id = source_row.session_id
      WHERE source_row.status = 'CURRENT'
      GROUP BY source_row.candidate_id, source_row.source_publication_id, source_row.source_ordinal
      HAVING pg_catalog.count(*) <> 1
    )
    SELECT pg_catalog.jsonb_build_object(
      'historical_source_rows', (
        SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(source_row) ORDER BY source_row.id), '[]'::jsonb)
        FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        WHERE source_row.id = ANY(ARRAY[${sourceHistoryIds}]::uuid[])
      ),
      'historical_preview_rows', (
        SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(preview_row) ORDER BY preview_row.id), '[]'::jsonb)
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        WHERE preview_row.id = ANY(ARRAY[${previewHistoryIds}]::uuid[])
      ),
      'duplicate_current_ordinal_count', (
        SELECT pg_catalog.count(*)::integer FROM duplicate_current_ordinals
      ),
      'active_sibling_job_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.banking_pay_workbench_jobs AS job_row
        WHERE job_row.candidate_id IN (SELECT candidate_id FROM session_candidates)
          AND job_row.status IN ('QUEUED', 'RUNNING')
      ),
      'active_sibling_build_count', (
        SELECT pg_catalog.count(*)::integer
        FROM private.banking_pay_workbench_economic_builds AS build_row
        WHERE build_row.candidate_id IN (SELECT candidate_id FROM session_candidates)
          AND build_row.status NOT IN ('COMPLETE', 'FAILED', 'OBSOLETE', 'CLEANING')
      )
    )::text
  `).value;
  assert.equal(sha256(after.historical_source_rows), sourceHistorySha256, JSON.stringify(after));
  assert.equal(sha256(after.historical_preview_rows), previewHistorySha256, JSON.stringify(after));
  assert.equal(after.duplicate_current_ordinal_count, 0, JSON.stringify(after));
  assert.equal(after.active_sibling_job_count, 0, JSON.stringify(after));
  assert.equal(after.active_sibling_build_count, 0, JSON.stringify(after));
  selectedBatchCurrentness.h12_history_preserved = true;
  selectedBatchCurrentness.h12_history_preservation_boundary = 'AFTER_COMPLETE_SESSION_CONVERGENCE';
  selectedBatchCurrentness.h12_source_history_sha256 = sourceHistorySha256;
  selectedBatchCurrentness.h12_preview_history_sha256 = previewHistorySha256;
  selectedBatchCurrentness.h12_current_publication_ordinals_unique = true;
  selectedBatchCurrentness.h12_sibling_workbench_quiescent = true;
  selectedBatchCurrentness.h12_complete_session_convergence = completeSessionConvergence;
  selectedBatchCurrentness.h12_quiescent_session_replay = quiescentSessionReplay;
  selectedBatchCurrentness.h12_quiescent_replay_proved_no_work = true;
  return selectedBatchCurrentness;
}

function readH12PreparedSnapshotBoundary(target) {
  const fixtureAbsence = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'source_less_fixture', pg_catalog.to_regclass('private.h12_source_less_cancellation_fixture_v1'),
      'mail_fixture', pg_catalog.to_regclass('private.h12_source_less_cancellation_mail_fixture_v1')
    )::text
  `).value;
  assert.equal(fixtureAbsence.source_less_fixture, null, JSON.stringify(fixtureAbsence));
  assert.equal(fixtureAbsence.mail_fixture, null, JSON.stringify(fixtureAbsence));

  const perChannel = {};
  for (const channel of ['PAYE', 'UMBRELLA']) {
    const batchId = target.batches[channel];
    const productionShape = assertProductionShapedWorkbenchComponents(target, batchId);
    const invariant = assertCancellationWorkbenchPreScheduleInvariant(target, batchId);
    const semantic = queryJson(target, `
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
        JOIN candidate_scope AS candidate_row
          ON candidate_row.id = item_row.pay_batch_candidate_id
      ), session_scope AS (
        SELECT batch_row.source_workbench_session_id AS session_id
        FROM batch_scope AS batch_row
      ), session_candidates AS (
        SELECT scope_row.candidate_id
        FROM public.banking_pay_workbench_session_scope AS scope_row
        JOIN session_scope ON session_scope.session_id = scope_row.session_id
      )
      SELECT pg_catalog.jsonb_build_object(
        'batch', (
          SELECT pg_catalog.to_jsonb(batch_row)
            - ARRAY['created_at', 'created_at_utc', 'updated_at', 'updated_at_utc']::text[]
          FROM batch_scope AS batch_row
        ),
        'candidates', (
          SELECT COALESCE(pg_catalog.jsonb_agg(
            pg_catalog.to_jsonb(candidate_row)
              - ARRAY['created_at', 'created_at_utc', 'updated_at', 'updated_at_utc']::text[]
            ORDER BY candidate_row.id
          ), '[]'::jsonb)
          FROM candidate_scope AS candidate_row
        ),
        'items', (
          SELECT COALESCE(pg_catalog.jsonb_agg(
            pg_catalog.to_jsonb(item_row)
              - ARRAY['created_at', 'created_at_utc', 'updated_at', 'updated_at_utc']::text[]
            ORDER BY item_row.id
          ), '[]'::jsonb)
          FROM item_scope AS item_row
        ),
        'source_status_counts', (
          SELECT COALESCE(pg_catalog.jsonb_object_agg(status_rows.status, status_rows.row_count), '{}'::jsonb)
          FROM (
            SELECT source_row.status, pg_catalog.count(*)::integer AS row_count
            FROM public.banking_pay_workbench_candidate_source_lines AS source_row
            JOIN session_scope ON session_scope.session_id = source_row.session_id
            GROUP BY source_row.status
            ORDER BY source_row.status
          ) AS status_rows
        ),
        'preview_status_counts', (
          SELECT COALESCE(pg_catalog.jsonb_object_agg(status_rows.status, status_rows.row_count), '{}'::jsonb)
          FROM (
            SELECT preview_row.status, pg_catalog.count(*)::integer AS row_count
            FROM public.banking_pay_workbench_preview_rows AS preview_row
            JOIN session_scope ON session_scope.session_id = preview_row.session_id
            GROUP BY preview_row.status
            ORDER BY preview_row.status
          ) AS status_rows
        ),
        'job_status_counts', (
          SELECT COALESCE(pg_catalog.jsonb_object_agg(status_rows.status, status_rows.row_count), '{}'::jsonb)
          FROM (
            SELECT job_row.status, pg_catalog.count(*)::integer AS row_count
            FROM public.banking_pay_workbench_jobs AS job_row
            WHERE job_row.candidate_id IN (SELECT candidate_id FROM session_candidates)
            GROUP BY job_row.status
            ORDER BY job_row.status
          ) AS status_rows
        ),
        'build_status_counts', (
          SELECT COALESCE(pg_catalog.jsonb_object_agg(status_rows.status, status_rows.row_count), '{}'::jsonb)
          FROM (
            SELECT build_row.status, pg_catalog.count(*)::integer AS row_count
            FROM private.banking_pay_workbench_economic_builds AS build_row
            JOIN session_scope ON session_scope.session_id = build_row.session_id
            GROUP BY build_row.status
            ORDER BY build_row.status
          ) AS status_rows
        ),
        'provider_attempt_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.banking_pay_operation_provider_attempts AS attempt_row
          WHERE attempt_row.pay_batch_id = '${batchId}'::uuid
        ),
        'provider_event_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.pay_bank_transfer_events AS event_row
          WHERE event_row.pay_batch_id = '${batchId}'::uuid
        ),
        'bank_transfer_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.pay_bank_transfers AS transfer_row
          WHERE transfer_row.pay_batch_id = '${batchId}'::uuid
        ),
        'provider_transfer_scope_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.banking_pay_operation_transfer_scope AS scope_row
          WHERE scope_row.pay_batch_id = '${batchId}'::uuid
        ),
        'provider_effect_count', (
          SELECT pg_catalog.sum(effect_rows.effect_count)::integer
          FROM (
            SELECT pg_catalog.count(*)::integer AS effect_count
            FROM public.banking_pay_operation_provider_attempts AS attempt_row
            WHERE attempt_row.pay_batch_id = '${batchId}'::uuid
            UNION ALL
            SELECT pg_catalog.count(*)::integer
            FROM public.pay_bank_transfer_events AS event_row
            WHERE event_row.pay_batch_id = '${batchId}'::uuid
            UNION ALL
            SELECT pg_catalog.count(*)::integer
            FROM public.pay_bank_transfers AS transfer_row
            WHERE transfer_row.pay_batch_id = '${batchId}'::uuid
              AND (
                transfer_row.request_id IS NOT NULL
                OR transfer_row.rail_tx_id IS NOT NULL
                OR transfer_row.rail_state IS NOT NULL
                OR transfer_row.rail_meta_json IS NOT NULL
              )
            UNION ALL
            SELECT pg_catalog.count(*)::integer
            FROM public.banking_pay_operation_transfer_scope AS scope_row
            WHERE scope_row.pay_batch_id = '${batchId}'::uuid
              AND (
                scope_row.provider_submit_attempt_count <> 0
                OR scope_row.provider_submit_chunk_id IS NOT NULL
                OR scope_row.provider_idempotency_key IS NOT NULL
                OR scope_row.provider_request_id IS NOT NULL
                OR scope_row.provider_transaction_id IS NOT NULL
                OR scope_row.provider_request_prepared_at_utc IS NOT NULL
                OR scope_row.provider_request_sending_at_utc IS NOT NULL
                OR scope_row.provider_request_sent_at_utc IS NOT NULL
                OR scope_row.provider_response_at_utc IS NOT NULL
              )
          ) AS effect_rows
        ),
        'settlement_effect_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.banking_pay_operation_settlement_scope AS settlement_row
          WHERE settlement_row.pay_batch_id = '${batchId}'::uuid
        ),
        'remittance_effect_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.banking_pay_operation_remittance_scope AS remittance_row
          WHERE remittance_row.pay_batch_id = '${batchId}'::uuid
        ),
        'external_effect_count', (
          SELECT pg_catalog.sum(effect_rows.effect_count)::integer
          FROM (
            SELECT pg_catalog.count(*)::integer AS effect_count
            FROM public.banking_pay_operation_provider_attempts
            WHERE pay_batch_id = '${batchId}'::uuid
            UNION ALL SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfer_events WHERE pay_batch_id = '${batchId}'::uuid
            UNION ALL SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfers WHERE pay_batch_id = '${batchId}'::uuid
            UNION ALL SELECT pg_catalog.count(*)::integer FROM public.banking_pay_operation_transfer_scope WHERE pay_batch_id = '${batchId}'::uuid
            UNION ALL SELECT pg_catalog.count(*)::integer FROM public.banking_pay_operation_settlement_scope WHERE pay_batch_id = '${batchId}'::uuid
            UNION ALL SELECT pg_catalog.count(*)::integer FROM public.banking_pay_operation_remittance_scope WHERE pay_batch_id = '${batchId}'::uuid
          ) AS effect_rows
        ),
        'provider_attempts_absent', NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_provider_attempts AS attempt_row
          WHERE attempt_row.pay_batch_id = '${batchId}'::uuid
        ),
        'provider_events_absent', NOT EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS event_row
          WHERE event_row.pay_batch_id = '${batchId}'::uuid
        ),
        'provider_effects_absent', NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_provider_attempts AS attempt_row
          WHERE attempt_row.pay_batch_id = '${batchId}'::uuid
          UNION ALL
          SELECT 1
          FROM public.pay_bank_transfer_events AS event_row
          WHERE event_row.pay_batch_id = '${batchId}'::uuid
          UNION ALL
          SELECT 1
          FROM public.pay_bank_transfers AS transfer_row
          WHERE transfer_row.pay_batch_id = '${batchId}'::uuid
            AND (
              transfer_row.request_id IS NOT NULL
              OR transfer_row.rail_tx_id IS NOT NULL
              OR transfer_row.rail_state IS NOT NULL
              OR transfer_row.rail_meta_json IS NOT NULL
            )
          UNION ALL
          SELECT 1
          FROM public.banking_pay_operation_transfer_scope AS scope_row
          WHERE scope_row.pay_batch_id = '${batchId}'::uuid
            AND (
              scope_row.provider_submit_attempt_count <> 0
              OR scope_row.provider_submit_chunk_id IS NOT NULL
              OR scope_row.provider_idempotency_key IS NOT NULL
              OR scope_row.provider_request_id IS NOT NULL
              OR scope_row.provider_transaction_id IS NOT NULL
              OR scope_row.provider_request_prepared_at_utc IS NOT NULL
              OR scope_row.provider_request_sending_at_utc IS NOT NULL
              OR scope_row.provider_request_sent_at_utc IS NOT NULL
              OR scope_row.provider_response_at_utc IS NOT NULL
            )
        ),
        'settlement_effects_absent', NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_settlement_scope AS settlement_row
          WHERE settlement_row.pay_batch_id = '${batchId}'::uuid
        ),
        'remittance_effects_absent', NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_remittance_scope AS remittance_row
          WHERE remittance_row.pay_batch_id = '${batchId}'::uuid
        ),
        'external_effects_absent', NOT EXISTS (
          SELECT 1 FROM public.banking_pay_operation_provider_attempts WHERE pay_batch_id = '${batchId}'::uuid
          UNION ALL SELECT 1 FROM public.pay_bank_transfer_events WHERE pay_batch_id = '${batchId}'::uuid
          UNION ALL SELECT 1 FROM public.pay_bank_transfers WHERE pay_batch_id = '${batchId}'::uuid
          UNION ALL SELECT 1 FROM public.banking_pay_operation_transfer_scope WHERE pay_batch_id = '${batchId}'::uuid
          UNION ALL SELECT 1 FROM public.banking_pay_operation_settlement_scope WHERE pay_batch_id = '${batchId}'::uuid
          UNION ALL SELECT 1 FROM public.banking_pay_operation_remittance_scope WHERE pay_batch_id = '${batchId}'::uuid
        ),
        'correction_request_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.pay_payment_correction_requests AS request_row
          WHERE request_row.pay_batch_id = '${batchId}'::uuid
        ),
        'nonterminal_correction_request_count', (
          SELECT pg_catalog.count(*)::integer
          FROM public.pay_payment_correction_requests AS request_row
          WHERE request_row.pay_batch_id = '${batchId}'::uuid
            AND request_row.status NOT IN (
              'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
              'FAILED', 'REJECTED', 'CANCELLED'
            )
        )
      )::text
    `).value;
    assert.equal(semantic.batch.status, 'DRAFT', JSON.stringify(semantic.batch));
    assert.equal(semantic.batch.batch_kind_fixed, channel, JSON.stringify(semantic.batch));
    assert.equal(semantic.nonterminal_correction_request_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.provider_attempt_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.provider_event_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.bank_transfer_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.provider_transfer_scope_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.provider_effect_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.settlement_effect_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.remittance_effect_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.external_effect_count, 0, JSON.stringify(semantic));
    assert.equal(semantic.provider_attempts_absent, true, JSON.stringify(semantic));
    assert.equal(semantic.provider_events_absent, true, JSON.stringify(semantic));
    assert.equal(semantic.provider_effects_absent, true, JSON.stringify(semantic));
    assert.equal(semantic.settlement_effects_absent, true, JSON.stringify(semantic));
    assert.equal(semantic.remittance_effects_absent, true, JSON.stringify(semantic));
    assert.equal(semantic.external_effects_absent, true, JSON.stringify(semantic));
    perChannel[channel] = { production_shape: productionShape, invariant, semantic };
  }

  const exactState = queryJson(target, `
    CREATE OR REPLACE FUNCTION pg_temp.h12_relation_manifest_v1()
    RETURNS jsonb
    LANGUAGE plpgsql
    AS $h12_manifest$
    DECLARE
      relation_row record;
      relation_count bigint;
      relation_hash text;
      relation_manifest jsonb := '{}'::jsonb;
    BEGIN
      FOR relation_row IN
        SELECT namespace_row.nspname AS schema_name,
               class_row.relname AS relation_name
        FROM pg_catalog.pg_class AS class_row
        JOIN pg_catalog.pg_namespace AS namespace_row
          ON namespace_row.oid = class_row.relnamespace
        WHERE namespace_row.nspname IN ('auth', 'private', 'public')
          AND class_row.relkind IN ('r', 'p')
        ORDER BY namespace_row.nspname, class_row.relname
      LOOP
        EXECUTE pg_catalog.format(
          $h12_relation$
            SELECT pg_catalog.count(*)::bigint,
                   pg_catalog.md5(COALESCE(
                     pg_catalog.string_agg(hashed_row.row_hash, '' ORDER BY hashed_row.row_hash),
                     ''
                   ))
            FROM (
              SELECT pg_catalog.md5(pg_catalog.to_jsonb(source_row)::text) AS row_hash
              FROM %I.%I AS source_row
            ) AS hashed_row
          $h12_relation$,
          relation_row.schema_name,
          relation_row.relation_name
        ) INTO relation_count, relation_hash;
        relation_manifest := relation_manifest || pg_catalog.jsonb_build_object(
          relation_row.schema_name || '.' || relation_row.relation_name,
          pg_catalog.jsonb_build_object('row_count', relation_count, 'row_multiset_md5', relation_hash)
        );
      END LOOP;
      RETURN relation_manifest;
    END
    $h12_manifest$;
    SELECT pg_catalog.jsonb_build_object(
      'relations', pg_temp.h12_relation_manifest_v1(),
      'sequences', COALESCE((
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.to_jsonb(sequence_row)
          ORDER BY sequence_row.schemaname, sequence_row.sequencename
        )
        FROM pg_catalog.pg_sequences AS sequence_row
        WHERE sequence_row.schemaname IN ('auth', 'private', 'public')
      ), '[]'::jsonb),
      'function_catalog_md5', (
        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          namespace_row.nspname || '.' || procedure_row.proname || '('
            || pg_catalog.pg_get_function_identity_arguments(procedure_row.oid) || ')=' 
            || pg_catalog.pg_get_functiondef(procedure_row.oid),
          E'\\n' ORDER BY namespace_row.nspname, procedure_row.proname,
            pg_catalog.pg_get_function_identity_arguments(procedure_row.oid)
        ), ''))
        FROM pg_catalog.pg_proc AS procedure_row
        JOIN pg_catalog.pg_namespace AS namespace_row
          ON namespace_row.oid = procedure_row.pronamespace
        WHERE namespace_row.nspname IN ('private', 'public')
          AND procedure_row.prokind IN ('f', 'p')
      ),
      'trigger_catalog_md5', (
        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          pg_catalog.pg_get_triggerdef(trigger_row.oid, true),
          E'\\n' ORDER BY namespace_row.nspname, class_row.relname, trigger_row.tgname
        ), ''))
        FROM pg_catalog.pg_trigger AS trigger_row
        JOIN pg_catalog.pg_class AS class_row ON class_row.oid = trigger_row.tgrelid
        JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid = class_row.relnamespace
        WHERE namespace_row.nspname IN ('private', 'public')
          AND trigger_row.tgisinternal IS FALSE
      ),
      'constraint_catalog_md5', (
        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          namespace_row.nspname || '.' || class_row.relname || '.' || constraint_row.conname
            || '=' || pg_catalog.pg_get_constraintdef(constraint_row.oid, true),
          E'\\n' ORDER BY namespace_row.nspname, class_row.relname, constraint_row.conname
        ), ''))
        FROM pg_catalog.pg_constraint AS constraint_row
        JOIN pg_catalog.pg_class AS class_row ON class_row.oid = constraint_row.conrelid
        JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid = class_row.relnamespace
        WHERE namespace_row.nspname IN ('private', 'public')
      )
    )::text
  `).value;
  const semanticManifest = {
    contract: 'H12_PRE_CANCELLATION_SEMANTIC_BOUNDARY_V1',
    source_fingerprint: H12_PREPARED_SOURCE_FINGERPRINT,
    fixture_absence: fixtureAbsence,
    channels: perChannel,
    function_catalog_md5: exactState.function_catalog_md5,
    trigger_catalog_md5: exactState.trigger_catalog_md5,
    constraint_catalog_md5: exactState.constraint_catalog_md5
  };
  const evidence = {
    contract: 'H12_PRE_CANCELLATION_PREPARED_SNAPSHOT_V1',
    boundary: 'AFTER_WORKBENCH_PRE_SCHEDULE_INVARIANT_BEFORE_SCHEDULED_PREPARATION',
    source_fingerprint: H12_PREPARED_SOURCE_FINGERPRINT,
    fixture_absent: true,
    route_mutation_started: false,
    immutable_template_source: true,
    relation_count: Object.keys(exactState.relations).length,
    sequence_count: exactState.sequences.length,
    semantic_sha256: sha256(semanticManifest),
    exact_sha256: sha256(exactState),
    semantic_manifest: semanticManifest,
    exact_manifest: exactState
  };
  const expectedSource = String(process.env.H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SOURCE || '').trim();
  const expectedSemantic = String(process.env.H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SEMANTIC_SHA256 || '').trim();
  const expectedExact = String(process.env.H12_SOURCE_LESS_EXPECTED_SNAPSHOT_EXACT_SHA256 || '').trim();
  if (expectedSource || expectedSemantic || expectedExact) {
    assert.equal(expectedSource, H12_PREPARED_SOURCE_FINGERPRINT, 'H12_PREPARED_SNAPSHOT_SOURCE_DRIFT');
    assert.match(expectedSemantic, /^[0-9a-f]{64}$/i, 'H12_PREPARED_SNAPSHOT_SEMANTIC_HASH_MISSING');
    assert.match(expectedExact, /^[0-9a-f]{64}$/i, 'H12_PREPARED_SNAPSHOT_EXACT_HASH_MISSING');
    assert.equal(evidence.semantic_sha256, expectedSemantic, 'H12_PREPARED_SNAPSHOT_SEMANTIC_DRIFT');
    assert.equal(evidence.exact_sha256, expectedExact, 'H12_PREPARED_SNAPSHOT_EXACT_DRIFT');
  }
  return evidence;
}

function prepareH12FreshPreCancellationBoundary(target) {
  assert.equal(
    String(process.env.H12_SOURCE_LESS_CAPTURE_PRE_CANCELLATION_BOUNDARY || '').trim().toLowerCase(),
    'true'
  );
  const anchors = queryJson(target, `
    WITH requested(channel, batch_id) AS (
      VALUES
        ('PAYE'::text, '${target.batches.PAYE}'::uuid),
        ('UMBRELLA'::text, '${target.batches.UMBRELLA}'::uuid)
    ), resolved AS (
      SELECT requested.channel,
             requested.batch_id,
             batch_row.source_workbench_session_id AS session_id,
             pg_catalog.row_number() OVER (
               PARTITION BY batch_row.source_workbench_session_id
               ORDER BY requested.channel, requested.batch_id
             ) AS session_ordinal
      FROM requested
      JOIN public.pay_batches AS batch_row ON batch_row.id = requested.batch_id
    )
    SELECT pg_catalog.jsonb_build_object(
      'requested_count', (SELECT pg_catalog.count(*)::integer FROM requested),
      'resolved_count', (SELECT pg_catalog.count(*)::integer FROM resolved),
      'anchors', COALESCE((
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'channel', resolved.channel,
            'batch_id', resolved.batch_id,
            'session_id', resolved.session_id
          ) ORDER BY resolved.session_id, resolved.channel, resolved.batch_id
        )
        FROM resolved
        WHERE resolved.session_ordinal = 1
      ), '[]'::jsonb)
    )::text
  `).value;
  assert.equal(anchors.requested_count, 2, JSON.stringify(anchors));
  assert.equal(anchors.resolved_count, 2, JSON.stringify(anchors));
  assert.ok(anchors.anchors.length >= 1 && anchors.anchors.length <= 2, JSON.stringify(anchors));
  const freshBoundaryRuns = [];
  for (const anchor of anchors.anchors) {
    const boundaryRun = runBaseCancellation(target, anchor.channel, anchor.batch_id);
    assert.equal(boundaryRun.h12_pre_cancellation_boundary_only, true, JSON.stringify(boundaryRun));
    freshBoundaryRuns.push({ ...anchor, boundary_run: boundaryRun });
  }
  return {
    ...readH12PreparedSnapshotBoundary(target),
    distinct_source_session_count: anchors.anchors.length,
    fresh_boundary_runs: freshBoundaryRuns,
    one_anchor_per_distinct_source_session: true,
    legacy_fresh_path_used: true
  };
}

function activateH12AmbiguityFixture(target) {
  const channel = String(process.env.H2_CANCEL_CHANNEL || '').trim().toUpperCase();
  assert.ok(channel === 'PAYE' || channel === 'UMBRELLA');
  const expectedReasons = channel === 'PAYE'
    ? ['MISSING_AMOUNT_INC_VAT', 'MISSING_DESCRIPTION', 'ZERO_AMOUNT']
    : ['MISSING_AMOUNT_EX_VAT', 'MISSING_AMOUNT_VAT'];
  const activated = queryJson(target, `
    WITH changed AS (
      UPDATE public.pay_batch_items AS item_row
      SET
        description = CASE fixture_row.ambiguity_reason
          WHEN 'MISSING_DESCRIPTION' THEN ' '
          ELSE item_row.description
        END,
        amount_ex_vat = CASE fixture_row.ambiguity_reason
          WHEN 'MISSING_AMOUNT_EX_VAT' THEN NULL::numeric
          ELSE item_row.amount_ex_vat
        END,
        amount_vat = CASE fixture_row.ambiguity_reason
          WHEN 'MISSING_AMOUNT_VAT' THEN NULL::numeric
          ELSE item_row.amount_vat
        END,
        amount_inc_vat = CASE fixture_row.ambiguity_reason
          WHEN 'MISSING_AMOUNT_INC_VAT' THEN NULL::numeric
          WHEN 'ZERO_AMOUNT' THEN 0::numeric
          ELSE item_row.amount_inc_vat
        END,
        umbrella_id = item_row.umbrella_id
      FROM private.h12_source_less_cancellation_fixture_v1 AS fixture_row
      WHERE fixture_row.pay_batch_item_id = item_row.id
        AND fixture_row.fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
      RETURNING item_row.*
    ), refreshed_expected AS (
      UPDATE private.h12_source_less_cancellation_fixture_v1 AS fixture_row
      SET expected_description = item_row.description,
          expected_amount_ex_vat = item_row.amount_ex_vat,
          expected_amount_vat = item_row.amount_vat,
          expected_amount_inc_vat = item_row.amount_inc_vat,
          expected_paye_treatment = item_row.paye_treatment
      FROM changed AS item_row
      WHERE fixture_row.pay_batch_item_id = item_row.id
        AND fixture_row.fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
      RETURNING fixture_row.pay_batch_item_id
    )
    SELECT pg_catalog.jsonb_build_object(
      'changed_count', (SELECT pg_catalog.count(*)::integer FROM changed),
      'refreshed_expected_count', (SELECT pg_catalog.count(*)::integer FROM refreshed_expected)
    )::text
  `).value;
  const candidateCount = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'count', pg_catalog.count(DISTINCT fixture_row.pay_batch_candidate_id)::integer
    )::text
    FROM private.h12_source_less_cancellation_fixture_v1 AS fixture_row
  `).value.count;
  assert.equal(activated.changed_count, candidateCount * expectedReasons.length, JSON.stringify(activated));
  assert.equal(activated.refreshed_expected_count, candidateCount * expectedReasons.length, JSON.stringify(activated));
  const detector = queryJson(target, `
    WITH candidate_scope AS (
      SELECT fixture_row.pay_batch_candidate_id,
             pg_catalog.array_agg(fixture_row.pay_batch_item_id ORDER BY fixture_row.pay_batch_item_id) AS item_ids
      FROM private.h12_source_less_cancellation_fixture_v1 AS fixture_row
      GROUP BY fixture_row.pay_batch_candidate_id
    ), checked AS (
      SELECT candidate_scope.pay_batch_candidate_id,
             public._pay_detect_manual_adjustments_for_carry_forward(
               '${target.batches[channel]}'::uuid,
               pg_catalog.jsonb_build_object('pay_batch_item_ids', pg_catalog.to_jsonb(candidate_scope.item_ids)),
               '${ACTOR_ID}'::uuid
             ) AS result
      FROM candidate_scope
    )
    SELECT pg_catalog.jsonb_build_object(
      'candidate_count', pg_catalog.count(*)::integer,
      'source_backed_minimum', pg_catalog.min(COALESCE((result->>'source_backed_count')::integer, 0)),
      'safe_minimum', pg_catalog.min(COALESCE((result->>'source_less_safe_count')::integer, 0)),
      'safe_maximum', pg_catalog.max(COALESCE((result->>'source_less_safe_count')::integer, 0)),
      'ambiguous_minimum', pg_catalog.min(COALESCE((result->>'source_less_ambiguous_count')::integer, 0)),
      'ambiguous_maximum', pg_catalog.max(COALESCE((result->>'source_less_ambiguous_count')::integer, 0)),
      'automatic_count', pg_catalog.count(*) FILTER (
        WHERE COALESCE((result->>'can_carry_forward_automatically')::boolean, true) IS TRUE
      )::integer,
      'reason_sets', pg_catalog.jsonb_agg((
        SELECT pg_catalog.jsonb_agg(blocker.value->>'reason' ORDER BY blocker.value->>'reason')
        FROM pg_catalog.jsonb_array_elements(COALESCE(checked.result->'carry_forward_blockers', '[]'::jsonb)) AS blocker(value)
      ) ORDER BY checked.pay_batch_candidate_id)
    )::text
    FROM checked
  `).value;
  assert.equal(detector.candidate_count, candidateCount, JSON.stringify(detector));
  assert.ok(detector.source_backed_minimum >= 1, JSON.stringify(detector));
  assert.equal(detector.safe_minimum, 1, JSON.stringify(detector));
  assert.equal(detector.safe_maximum, 1, JSON.stringify(detector));
  assert.equal(detector.ambiguous_minimum, expectedReasons.length, JSON.stringify(detector));
  assert.equal(detector.ambiguous_maximum, expectedReasons.length, JSON.stringify(detector));
  assert.equal(detector.automatic_count, 0, JSON.stringify(detector));
  for (const reasonSet of detector.reason_sets) {
    assert.deepEqual(reasonSet, [...expectedReasons].sort(), 'H12_DETECTOR_REASON_SET_MISMATCH');
  }
  return { ...activated, detector };
}

function convergeH12FixtureCreatedWorkbenchJobs(target, batchId) {
  const paymentState = String(process.env.H2_CANCEL_PAYMENT_STATE || 'DRAFT').trim().toUpperCase();
  const expectedReasonSet = paymentState.endsWith('_FAILED_NO_MONEY')
    ? [
        'DIRTY_TRIGGER:PAY_BANK_TRANSFERS:UPDATE',
        'PAY_BANK_TRANSFER_EVENTS_INSERT',
        'PAY_BANK_TRANSFER_EVENTS_UPDATE',
        'PAY_BATCH_ITEMS_INSERT',
        'PAY_BATCH_ITEMS_UPDATE'
      ]
    : ['PAY_BATCH_ITEMS_INSERT', 'PAY_BATCH_ITEMS_UPDATE'];
  const before = queryJson(target, `
    WITH batch_candidates AS (
      SELECT candidate_row.candidate_id
      FROM public.pay_batch_candidates AS candidate_row
      WHERE candidate_row.pay_batch_id = '${batchId}'::uuid
    ), active_jobs AS (
      SELECT job_row.*
      FROM public.banking_pay_workbench_jobs AS job_row
      WHERE job_row.candidate_id IN (SELECT candidate_id FROM batch_candidates)
        AND job_row.status IN ('QUEUED', 'RUNNING')
    )
    SELECT pg_catalog.jsonb_build_object(
      'candidate_ids', (
        SELECT COALESCE(
          pg_catalog.jsonb_agg(candidate_id::text ORDER BY candidate_id),
          '[]'::jsonb
        )
        FROM batch_candidates
      ),
      'active_jobs', (
        SELECT COALESCE(
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'id', active_job.id::text,
              'candidate_id', active_job.candidate_id::text,
              'job_type', active_job.job_type,
              'status', active_job.status,
              'reason', active_job.payload_json->>'reason',
              'reasons', COALESCE(active_job.payload_json->'reasons', '[]'::jsonb),
              'created_at_utc', active_job.created_at_utc
            ) ORDER BY active_job.candidate_id, active_job.created_at_utc, active_job.id
          ),
          '[]'::jsonb
        )
        FROM active_jobs AS active_job
      ),
      'active_nonterminal_correction_request_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_requests AS request_row
        WHERE request_row.pay_batch_id = '${batchId}'::uuid
          AND request_row.status NOT IN (
            'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
            'FAILED', 'REJECTED', 'CANCELLED'
          )
      )
    )::text
  `).value;
  assert.ok(Array.isArray(before.candidate_ids) && before.candidate_ids.length > 0, JSON.stringify(before));
  assert.equal(before.active_nonterminal_correction_request_count, 0, JSON.stringify(before));
  assert.ok(before.active_jobs.length >= before.candidate_ids.length, JSON.stringify(before));
  assert.ok(before.active_jobs.length <= before.candidate_ids.length * 2, JSON.stringify(before));
  for (const candidateId of before.candidate_ids) {
    const candidateJobs = before.active_jobs.filter((job) => job.candidate_id === candidateId);
    assert.ok(candidateJobs.length >= 1 && candidateJobs.length <= 2, JSON.stringify({ candidateId, candidateJobs }));
    const observedReasons = new Set();
    for (const job of candidateJobs) {
      assert.equal(job.job_type, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', JSON.stringify(job));
      assert.equal(job.status, 'QUEUED', JSON.stringify(job));
      assert.ok(Array.isArray(job.reasons) && job.reasons.length > 0, JSON.stringify(job));
      assert.ok(job.reasons.includes(job.reason), JSON.stringify(job));
      for (const reason of job.reasons) {
        assert.ok(expectedReasonSet.includes(reason), JSON.stringify(job));
        observedReasons.add(reason);
      }
    }
    assert.deepEqual([...observedReasons].sort(), [...expectedReasonSet].sort(), JSON.stringify({ candidateId, candidateJobs }));
  }
  const convergence = drainCancellationWorkbenchSourceBuilds(target, batchId);
  const after = assertCancellationWorkbenchPreScheduleInvariant(target, batchId);
  return {
    ...before,
    convergence,
    after,
    setup_jobs_converged_through_worker_order: true,
    normal_lane_quiescent_before_source_lane: true,
    correction_request_created_after_convergence: true,
    succeeded_failed_audit_history_preserved: true
  };
}

function startH12WholeNoMoneyCancellation(target, batchId, requestedAction, correctionReason, channel) {
  const filterJson = "pg_catalog.jsonb_build_object('action','RELEASE_FAILED_PAYMENT','actionable_only',true)";
  const page = queryJson(target, `
    SELECT public.pay_batch_payment_status_page_v1(
      '${batchId}'::uuid,
      '${ACTOR_ID}'::uuid,
      ${filterJson},
      'STATUS',
      'ASC',
      100,
      NULL::jsonb
    )::text
  `).value;
  assert.equal(page.ok, true, JSON.stringify(page));
  assert.ok(Array.isArray(page.rows) && page.rows.length > 0, JSON.stringify(page));
  assert.match(page.snapshot_token, /^[0-9a-f]{64}$/i);
  const batchCandidateCount = queryJson(target, `
    SELECT pg_catalog.jsonb_build_object(
      'count', pg_catalog.count(*)::integer
    )::text
    FROM public.pay_batch_candidates
    WHERE pay_batch_id = '${batchId}'::uuid
  `).value.count;
  assert.equal(page.rows.length, batchCandidateCount, JSON.stringify(page));
  return queryJson(target, `
    SELECT public.pay_payment_correction_request_start(
      p_pay_batch_id := '${batchId}'::uuid,
      p_selection_json := pg_catalog.jsonb_build_object(
        'command','PREPARE',
        'context','CURRENT_PAYMENT_STATUS',
        'contract_version',1,
        'mode','ALL_MATCHING',
        'requested_action','${requestedAction}',
        'snapshot_token','${page.snapshot_token}',
        'sort_key','STATUS',
        'sort_direction','ASC',
        'filter_json',${filterJson},
        'exclusions','[]'::jsonb,
        'idempotency_key','h12-whole-no-money-${target.database}-${channel.toLowerCase()}'
      ),
      p_reason := '${correctionReason}',
      p_actor_user_id := '${ACTOR_ID}'::uuid,
      p_source_bank_event_id := NULL::uuid,
      p_auto_requested := false,
      p_accepted_resolution_json := NULL::jsonb
    )::text
  `);
}

function readH12Evidence(target, batchId, requestId, operationId) {
  return queryJson(target, `
    WITH current_request AS (
      SELECT request_row.*
      FROM public.pay_payment_correction_requests AS request_row
      WHERE request_row.id = '${requestId}'::uuid
    ), selected_fixture AS (
      SELECT fixture_row.*, item_row.is_voided
      FROM private.h12_source_less_cancellation_fixture_v1 AS fixture_row
      JOIN public.pay_batch_items AS item_row
        ON item_row.id = fixture_row.pay_batch_item_id
      WHERE item_row.is_voided IS TRUE
    ), carry_forward_scope AS (
      SELECT carry_forward_row.*
      FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_row
      JOIN selected_fixture AS fixture_row
        ON fixture_row.pay_batch_item_id = carry_forward_row.source_pay_batch_item_id
    ), correction_evidence AS (
      SELECT correction_row.*, fixture_row.fixture_kind,
             fixture_row.ambiguity_reason,
             fixture_row.expected_pay_channel,
             fixture_row.expected_description,
             fixture_row.expected_amount_ex_vat,
             fixture_row.expected_amount_vat,
             fixture_row.expected_amount_inc_vat,
             fixture_row.expected_paye_treatment
      FROM public.pay_payment_correction_items AS correction_row
      JOIN current_request AS request_row
        ON request_row.id = correction_row.correction_request_id
      JOIN selected_fixture AS fixture_row
        ON fixture_row.pay_batch_item_id = correction_row.pay_batch_item_id
      WHERE correction_row.status = 'APPLIED'
    ), alert_document AS (
      SELECT public.banking_alerts_active_for_user(
        '${ACTOR_ID}'::uuid,
        NULL::text,
        NULL::uuid,
        true,
        500,
        'ALERT_PANEL'
      ) AS value
    ), alert_rows AS (
      SELECT alert_row.value
      FROM alert_document
      CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
        COALESCE(alert_document.value->'alerts', '[]'::jsonb)
      ) AS alert_row(value)
    ), ambiguous_alert_matches AS (
      SELECT
        fixture_row.pay_batch_item_id,
        pg_catalog.count(alert_row.value)::integer AS matching_alert_count
      FROM selected_fixture AS fixture_row
      LEFT JOIN alert_rows AS alert_row
        ON alert_row.value::text LIKE '%' || fixture_row.pay_batch_item_id::text || '%'
       AND alert_row.value::text LIKE '%' || fixture_row.ambiguity_reason || '%'
       AND alert_row.value::text LIKE '%${batchId}%'
      WHERE fixture_row.fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
      GROUP BY fixture_row.pay_batch_item_id
    ), mail_state AS (
      SELECT
        fixture_row.*,
        mail_row.id IS NOT NULL AS row_present,
        mail_row.status::text AS actual_status,
        mail_row.attempt_lease_token AS actual_attempt_lease_token,
        CASE WHEN mail_row.id IS NULL THEN NULL::text
          ELSE pg_catalog.md5(pg_catalog.to_jsonb(mail_row)::text)
        END AS actual_row_md5
      FROM private.h12_source_less_cancellation_mail_fixture_v1 AS fixture_row
      LEFT JOIN public.mail_outbox AS mail_row
        ON mail_row.id = fixture_row.mail_outbox_id
    )
    SELECT pg_catalog.jsonb_build_object(
      'request_id', request_row.id,
      'request_status', request_row.status,
      'candidate_scope_contract_version', NULLIF(request_row.plan_json->>'candidate_scope_contract_version', '')::integer,
      'candidate_scope_hash_version', NULLIF(request_row.plan_json->>'candidate_scope_hash_version', '')::integer,
      'source_row_count_semantics', request_row.plan_json->>'source_row_count_semantics',
      'communication_cleanup_contract_version', NULLIF(request_row.plan_json->>'communication_cleanup_contract_version', '')::integer,
      'work_item_communication_contract_mismatch_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS contract_work_item
        WHERE contract_work_item.correction_request_id = request_row.id
          AND NULLIF(contract_work_item.selection_json->>'communication_cleanup_contract_version', '')::integer IS DISTINCT FROM 2
      ),
      'applied_result_communication_contract_mismatch_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS contract_work_item
        WHERE contract_work_item.correction_request_id = request_row.id
          AND contract_work_item.status = 'APPLIED'
          AND NULLIF(contract_work_item.result_json->>'communication_cleanup_contract_version', '')::integer IS DISTINCT FROM 2
      ),
      'operation_status', operation_row.status,
      'operation_id', operation_row.id,
      'selected_fixture_candidate_count', (
        SELECT pg_catalog.count(DISTINCT fixture_row.pay_batch_candidate_id)::integer
        FROM selected_fixture AS fixture_row
      ),
      'selected_fixture_item_count', (
        SELECT pg_catalog.count(*)::integer FROM selected_fixture
      ),
      'selected_safe_count', (
        SELECT pg_catalog.count(*)::integer FROM selected_fixture
        WHERE fixture_kind = 'SOURCE_LESS_SAFE'
      ),
      'selected_ambiguous_count', (
        SELECT pg_catalog.count(*)::integer FROM selected_fixture
        WHERE fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
      ),
      'selected_ordinary_count', (
        SELECT pg_catalog.count(*)::integer FROM selected_fixture
        WHERE fixture_kind = 'ORDINARY_SOURCE_BACKED'
      ),
      'safe_carry_forward_count', (
        SELECT pg_catalog.count(*)::integer FROM carry_forward_scope
        WHERE source_pay_batch_item_id IN (
          SELECT pay_batch_item_id FROM selected_fixture
          WHERE fixture_kind = 'SOURCE_LESS_SAFE'
        )
      ),
      'ambiguous_carry_forward_count', (
        SELECT pg_catalog.count(*)::integer FROM carry_forward_scope
        WHERE source_pay_batch_item_id IN (
          SELECT pay_batch_item_id FROM selected_fixture
          WHERE fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
        )
      ),
      'correction_evidence_count', (
        SELECT pg_catalog.count(*)::integer FROM correction_evidence
      ),
      'ambiguous_correction_evidence_count', (
        SELECT pg_catalog.count(*)::integer FROM correction_evidence
        WHERE fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
      ),
      'exact_before_snapshot_count', (
        SELECT pg_catalog.count(*)::integer
        FROM correction_evidence
        WHERE before_snapshot_json->>'id' = pay_batch_item_id::text
          AND before_snapshot_json->>'pay_channel' = expected_pay_channel
          AND (before_snapshot_json->>'description') IS NOT DISTINCT FROM expected_description
          AND NULLIF(before_snapshot_json->>'amount_ex_vat','')::numeric IS NOT DISTINCT FROM expected_amount_ex_vat
          AND NULLIF(before_snapshot_json->>'amount_vat','')::numeric IS NOT DISTINCT FROM expected_amount_vat
          AND NULLIF(before_snapshot_json->>'amount_inc_vat','')::numeric IS NOT DISTINCT FROM expected_amount_inc_vat
          AND (before_snapshot_json->>'paye_treatment') IS NOT DISTINCT FROM expected_paye_treatment
      ),
      'ambiguous_workbench_reappearance_count', (
        SELECT pg_catalog.count(*)::integer
        FROM selected_fixture AS fixture_row
        JOIN public.banking_pay_workbench_preview_rows AS preview_row
          ON preview_row.row_json::text LIKE '%' || fixture_row.pay_batch_item_id::text || '%'
        WHERE fixture_row.fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
      ),
      'safe_workbench_reappearance_count', (
        SELECT pg_catalog.count(DISTINCT carry_forward_row.source_pay_batch_item_id)::integer
        FROM carry_forward_scope AS carry_forward_row
        JOIN public.banking_pay_workbench_preview_rows AS preview_row
          ON preview_row.row_json::text LIKE '%' || carry_forward_row.id::text || '%'
        WHERE carry_forward_row.source_pay_batch_item_id IN (
          SELECT pay_batch_item_id FROM selected_fixture
          WHERE fixture_kind = 'SOURCE_LESS_SAFE'
        )
          AND preview_row.status = 'READY'
      ),
      'ambiguous_alert_evidence_count', (
        SELECT pg_catalog.count(*)::integer
        FROM ambiguous_alert_matches
        WHERE matching_alert_count = 1
      ),
      'ambiguous_alert_wrong_multiplicity_count', (
        SELECT pg_catalog.count(*)::integer
        FROM ambiguous_alert_matches
        WHERE matching_alert_count <> 1
      ),
      'alert_contains_sensitive_bank_key', EXISTS (
        SELECT 1
        FROM alert_rows
        WHERE pg_catalog.lower(value::text) ~ '(account_number|sort_code|iban|auth_header|access_token)'
          AND value::text LIKE '%${batchId}%'
      ),
      'manual_support_work_item_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS work_item_row
        WHERE work_item_row.correction_request_id = request_row.id
          AND work_item_row.status = 'APPLIED'
          AND work_item_row.result_json ? 'manual_adjustment_support_details_json'
      ),
      'mail_fixture_count', (
        SELECT pg_catalog.count(*)::integer FROM mail_state
      ),
      'mail_fixture_kind_count', (
        SELECT pg_catalog.count(DISTINCT fixture_kind)::integer FROM mail_state
      ),
      'mail_changed_or_missing_count', (
        SELECT pg_catalog.count(*)::integer
        FROM mail_state
        WHERE row_present IS NOT TRUE
           OR actual_row_md5 IS DISTINCT FROM immutable_row_md5
      ),
      'mail_status_mismatch_count', (
        SELECT pg_catalog.count(*)::integer
        FROM mail_state
        WHERE actual_status IS DISTINCT FROM expected_status
      ),
      'mail_lease_mismatch_count', (
        SELECT pg_catalog.count(*)::integer
        FROM mail_state
        WHERE actual_attempt_lease_token IS DISTINCT FROM expected_attempt_lease_token
      ),
      'mail_claimed_fixture_count', (
        SELECT pg_catalog.count(*)::integer
        FROM mail_state
        WHERE fixture_kind = 'UNSAFE_RELATED_CLAIMED'
          AND actual_status = 'QUEUED'
          AND actual_attempt_lease_token = 'h12-mail-claimed-before-cancel'
      ),
      'mail_sent_fixture_count', (
        SELECT pg_catalog.count(*)::integer
        FROM mail_state
        WHERE fixture_kind = 'RELATED_SENT'
          AND actual_status = 'SENT'
      )
    )::text
    FROM current_request AS request_row
    JOIN public.banking_pay_operations AS operation_row
      ON operation_row.id = '${operationId}'::uuid
  `).value;
}

function readH12WholeBatchCancellationFinancials(target, batchId, requestId) {
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
                   reservation_row.status, reservation_row.reserved_amount,
                   reservation_row.reserved_source_amount,
                   reservation_row.frozen_rounded_target_amount,
                   reservation_row.committed_at_utc, reservation_row.settled_at_utc,
                   reservation_row.released_at_utc
                 ) ORDER BY reservation_row.id)
                 FROM public.pay_advance_reservations AS reservation_row
                 WHERE reservation_row.pay_batch_candidate_id = candidate_row.id
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
                 )
               ), '[]'::jsonb)
             ) AS authority_json
      FROM public.pay_batch_candidates AS candidate_row
      WHERE candidate_row.pay_batch_id = '${batchId}'::uuid
    ), correction_authority AS (
      SELECT correction_row.id,
             pg_catalog.jsonb_build_array(
               correction_row.id, correction_row.pay_batch_candidate_id,
               correction_row.pay_batch_item_id, correction_row.pay_bank_transfer_id,
               correction_row.finance_case_id, correction_row.finance_component_id,
               correction_row.reservation_id, correction_row.item_type,
               correction_row.correction_item_kind, correction_row.source_amount,
               correction_row.amount_ex_vat, correction_row.amount_vat,
               correction_row.amount_inc_vat, correction_row.economic_key_type,
               correction_row.economic_key_value, correction_row.before_snapshot_json,
               correction_row.after_snapshot_json, correction_row.status,
               correction_row.applied_at_utc
             ) AS authority_json
      FROM public.pay_payment_correction_items AS correction_row
      WHERE correction_row.correction_request_id = '${requestId}'::uuid
    )
    SELECT pg_catalog.jsonb_build_object(
      'candidate_count', (SELECT pg_catalog.count(*)::integer FROM candidate_authority),
      'candidate_authority_digest_sha256', private.pay_payment_correction_sha256_v1(
        COALESCE((SELECT pg_catalog.jsonb_agg(authority_json ORDER BY id) FROM candidate_authority), '[]'::jsonb)
      ),
      'correction_item_count', (SELECT pg_catalog.count(*)::integer FROM correction_authority),
      'correction_authority_digest_sha256', private.pay_payment_correction_sha256_v1(
        COALESCE((SELECT pg_catalog.jsonb_agg(authority_json ORDER BY id) FROM correction_authority), '[]'::jsonb)
      ),
      'provider_attempt_count', (SELECT pg_catalog.count(*)::integer FROM public.banking_pay_operation_provider_attempts),
      'provider_event_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfer_events)
    )::text
  `).value;
}

function replayH12CompletedRequest(target, requestId, batchId) {
  const cancellationScope = String(
    process.env.H2_CANCEL_SCOPE || 'WHOLE_BATCH'
  ).trim().toUpperCase();
  const paymentState = String(
    process.env.H2_CANCEL_PAYMENT_STATE || 'DRAFT'
  ).trim().toUpperCase();
  if (cancellationScope === 'WHOLE_BATCH' && paymentState === 'DRAFT') {
    // Whole-Draft cancellation is not a direct retry of the lower-level
    // request-start RPC.  The public Worker first resolves the one exact
    // fixed-shape pay_batch_cancel request/operation pair, including its
    // immutable actor, batch, reason, action and empty-filter identity.  Prove
    // that the committed database rows satisfy that real replay lookup and
    // that it resolves only the request created by this run.
    const exactReplay = queryJson(target, `
      WITH exact_pairs AS (
        SELECT
          request_row.id AS correction_request_id,
          request_row.status AS request_status,
          operation_row.id AS operation_id,
          operation_row.status AS operation_status,
          operation_row.phase AS operation_phase
        FROM public.pay_payment_correction_requests AS request_row
        JOIN public.banking_pay_operations AS operation_row
          ON operation_row.operation_type = 'PAYMENT_CORRECTION'
         AND operation_row.pay_batch_id = request_row.pay_batch_id
         AND operation_row.input_json->>'correction_request_id' = request_row.id::text
        WHERE request_row.pay_batch_id = '${batchId}'::uuid
          AND request_row.status IN (
            'PLANNING','PLANNED','REQUESTED','AWAITING_AUTHORISATION',
            'AUTHORISED','EXPANDED','PROCESSING','APPLIED','APPLIED_WITH_BLOCKERS'
          )
          AND request_row.correction_kind = 'PRE_BANK_CANCEL'
          AND request_row.requested_by_user_id = '${ACTOR_ID}'::uuid
          AND request_row.reason = 'DRAFT_PAYMENT_CANCELLED_BY_USER'
          AND request_row.source_bank_event_id IS NULL
          AND request_row.auto_requested IS FALSE
          AND pg_catalog.upper(COALESCE(
            request_row.selection_json->>'requested_action',
            request_row.selection_json#>>'{selection,action}',
            ''
          )) = 'DRAFT_CANCEL'
          AND (
            NOT (request_row.selection_json ? 'scope_type')
            OR pg_catalog.upper(COALESCE(request_row.selection_json->>'scope_type', '')) = 'BATCH'
          )
          AND pg_catalog.upper(COALESCE(
            request_row.selection_json->>'mode',
            request_row.selection_json#>>'{selection,mode}',
            ''
          )) = 'ALL_MATCHING'
          AND request_row.selection_json->>'source_context' = 'pay_batch_cancel'
          AND request_row.selection_json->'filter_json' = '{}'::jsonb
          AND request_row.selection_json->'exclusions' = '[]'::jsonb
          AND request_row.plan_json->>'requested_action' = 'DRAFT_CANCEL'
          AND NULLIF(request_row.selection_hash, '') IS NOT NULL
          AND NULLIF(request_row.plan_hash, '') IS NOT NULL
          AND operation_row.actor_user_id = '${ACTOR_ID}'::uuid
          AND operation_row.input_json->>'requested_action' = 'DRAFT_CANCEL'
          AND operation_row.input_json->'auto_requested' = 'false'::jsonb
          AND operation_row.input_json->'source_bank_event_id' = 'null'::jsonb
      )
      SELECT pg_catalog.jsonb_build_object(
        'ok', pg_catalog.count(*) = 1,
        'existing_request', pg_catalog.count(*) = 1,
        'exact_pair_count', pg_catalog.count(*)::integer,
        'correction_request_id', pg_catalog.min(correction_request_id::text),
        'operation_id', pg_catalog.min(operation_id::text),
        'request_status', pg_catalog.min(request_status),
        'operation_status', pg_catalog.min(operation_status),
        'phase', pg_catalog.min(operation_phase),
        'code', 'DRAFT_PAYMENT_CANCELLATION_EXACT_REPLAY'
      )::text
      FROM exact_pairs
    `).value;
    assert.equal(exactReplay.exact_pair_count, 1, JSON.stringify(exactReplay));
    assert.equal(exactReplay.correction_request_id, requestId, JSON.stringify(exactReplay));
    return exactReplay;
  }
  return queryJson(target, `
    SELECT public.pay_payment_correction_request_start(
      request_row.pay_batch_id,
      request_row.selection_json,
      request_row.reason,
      '${ACTOR_ID}'::uuid,
      request_row.source_bank_event_id,
      request_row.auto_requested,
      request_row.accepted_resolution_json
    )::text
    FROM public.pay_payment_correction_requests AS request_row
    WHERE request_row.id = '${requestId}'::uuid
  `).value;
}

function readH12PersistedContractEvidence(target, requestId) {
  return queryJson(target, `
    WITH request_contract AS (
      SELECT pg_catalog.jsonb_build_object(
        'candidate_scope_contract_version', NULLIF(request_row.plan_json->>'candidate_scope_contract_version', '')::integer,
        'candidate_scope_hash_version', NULLIF(request_row.plan_json->>'candidate_scope_hash_version', '')::integer,
        'source_row_count_semantics', request_row.plan_json->>'source_row_count_semantics',
        'communication_cleanup_contract_version', NULLIF(request_row.plan_json->>'communication_cleanup_contract_version', '')::integer
      ) AS value
      FROM public.pay_payment_correction_requests AS request_row
      WHERE request_row.id = '${requestId}'::uuid
    ), work_item_contracts AS (
      SELECT COALESCE(pg_catalog.jsonb_agg(contract_row.value ORDER BY contract_row.value::text), '[]'::jsonb) AS value
      FROM (
        SELECT DISTINCT pg_catalog.jsonb_build_object(
          'candidate_scope_contract_version', NULLIF(work_item_row.selection_json->>'candidate_scope_contract_version', '')::integer,
          'candidate_scope_hash_version', NULLIF(work_item_row.selection_json->>'candidate_scope_hash_version', '')::integer,
          'source_row_count_semantics', work_item_row.selection_json->>'source_row_count_semantics',
          'communication_cleanup_contract_version', NULLIF(work_item_row.selection_json->>'communication_cleanup_contract_version', '')::integer
        ) AS value
        FROM public.pay_payment_correction_work_items AS work_item_row
        WHERE work_item_row.correction_request_id = '${requestId}'::uuid
      ) AS contract_row
    )
    SELECT pg_catalog.jsonb_build_object(
      'request_contract', request_contract.value,
      'work_item_contracts', work_item_contracts.value
    )::text
    FROM request_contract
    CROSS JOIN work_item_contracts
  `).value;
}

function enableH12NoMoneyResultReturnCapture(target) {
  const enabled = queryJson(target, `
    UPDATE public.settings_defaults AS runtime_settings
    SET invoice_debug = true
    WHERE runtime_settings.id = 1
    RETURNING pg_catalog.jsonb_build_object(
      'enabled', runtime_settings.invoice_debug IS TRUE
    )::text
  `).value;
  assert.equal(enabled.enabled, true, 'H12 no-money returned-result capture was not enabled');
}

function readH12NoMoneyResultContractEvidence(target, requestId) {
  const expectedKeysJson = JSON.stringify(H12_NO_MONEY_RESULT_KEYS).replaceAll("'", "''");
  return queryJson(target, `
    WITH expected_keys AS (
      SELECT expected_key.value AS key
      FROM pg_catalog.jsonb_array_elements_text(
        '${expectedKeysJson}'::jsonb
      ) AS expected_key(value)
    ), applied_work AS (
      SELECT work_item_row.id, work_item_row.result_json
      FROM public.pay_payment_correction_work_items AS work_item_row
      WHERE work_item_row.correction_request_id = '${requestId}'::uuid
        AND work_item_row.work_kind = 'NO_MONEY_UNWIND'
        AND work_item_row.status = 'APPLIED'
    ), result_rows AS (
      SELECT
        applied_work.id AS work_item_id,
        returned_result.after_json AS returned_result,
        applied_work.result_json AS persisted_result,
        persisted_contract.value AS persisted_contract_result,
        (
          SELECT pg_catalog.count(*)::integer
          FROM public.audit_events AS capture_count
          WHERE capture_count.action = 'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT'
            AND capture_count.object_id_text = applied_work.id::text
        ) AS returned_result_capture_count
      FROM applied_work
      LEFT JOIN LATERAL (
        SELECT capture_row.after_json
        FROM public.audit_events AS capture_row
        WHERE capture_row.action = 'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT'
          AND capture_row.object_id_text = applied_work.id::text
        ORDER BY capture_row.ts_utc DESC, capture_row.id DESC
        LIMIT 1
      ) AS returned_result ON true
      CROSS JOIN LATERAL (
        SELECT COALESCE(
          pg_catalog.jsonb_object_agg(
            expected_key.key,
            applied_work.result_json->expected_key.key
            ORDER BY expected_key.key
          ) FILTER (WHERE applied_work.result_json ? expected_key.key),
          '{}'::jsonb
        ) AS value
        FROM expected_keys AS expected_key
      ) AS persisted_contract
    )
    SELECT pg_catalog.jsonb_build_object(
      'work_item_count', pg_catalog.count(*)::integer,
      'rows', COALESCE(
        pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'work_item_id', result_row.work_item_id,
            'returned_result_capture_count', result_row.returned_result_capture_count,
            'returned_result', result_row.returned_result,
            'persisted_result', result_row.persisted_result,
            'persisted_contract_result', result_row.persisted_contract_result,
            'returned_persisted_contract_jsonb_equal',
              result_row.returned_result IS NOT DISTINCT FROM result_row.persisted_contract_result,
            'returned_persisted_contract_canonical_text_equal',
              result_row.returned_result::text IS NOT DISTINCT FROM result_row.persisted_contract_result::text,
            'returned_result_sha256', private.pay_payment_correction_sha256_v1(result_row.returned_result),
            'persisted_contract_result_sha256', private.pay_payment_correction_sha256_v1(result_row.persisted_contract_result)
          )
          ORDER BY result_row.work_item_id
        ),
        '[]'::jsonb
      )
    )::text
    FROM result_rows AS result_row
  `).value;
}

function assertH12NoMoneyResultContract(evidence, expectedWorkItemCount, label) {
  const expectedKeys = [...H12_NO_MONEY_RESULT_KEYS].sort();
  const expectedPersistedKeys = [
    ...new Set([...expectedKeys, ...H12_NO_MONEY_PERSISTED_WRAPPER_KEYS])
  ].sort();
  assert.equal(expectedKeys.length, 82, `${label}: expected key contract is not 82 fields`);
  assert.equal(expectedPersistedKeys.length, 85, `${label}: persisted key contract is not 85 fields`);
  assert.equal(evidence.work_item_count, expectedWorkItemCount, `${label}: work-item count changed`);
  assert.equal(Array.isArray(evidence.rows), true, `${label}: result rows are missing`);
  assert.equal(evidence.rows.length, expectedWorkItemCount, `${label}: result row count changed`);

  for (const row of evidence.rows) {
    const rowLabel = `${label}:${row.work_item_id}`;
    assert.equal(row.returned_result_capture_count, 1, `${rowLabel}: exact returned-result capture count changed`);
    const returnedResult = row.returned_result;
    const persistedResult = row.persisted_result;
    const persistedContractResult = row.persisted_contract_result;
    assert.equal(
      returnedResult !== null && typeof returnedResult === 'object' && !Array.isArray(returnedResult),
      true,
      `${rowLabel}: returned result is not an object`
    );
    assert.equal(
      persistedResult !== null && typeof persistedResult === 'object' && !Array.isArray(persistedResult),
      true,
      `${rowLabel}: persisted result is not an object`
    );
    assert.deepEqual(Object.keys(returnedResult).sort(), expectedKeys, `${rowLabel}: returned key set changed`);
    assert.deepEqual(Object.keys(persistedResult).sort(), expectedPersistedKeys, `${rowLabel}: full persisted key set changed`);
    assert.deepEqual(Object.keys(persistedContractResult).sort(), expectedKeys, `${rowLabel}: persisted key set changed`);
    for (const key of expectedKeys) {
      assert.equal(Object.hasOwn(returnedResult, key), true, `${rowLabel}: returned key missing: ${key}`);
      assert.equal(Object.hasOwn(persistedResult, key), true, `${rowLabel}: persisted key missing: ${key}`);
      assert.equal(Object.hasOwn(persistedContractResult, key), true, `${rowLabel}: persisted projection key missing: ${key}`);
      if (returnedResult[key] === null || persistedResult[key] === null) {
        assert.equal(Object.hasOwn(returnedResult, key), true, `${rowLabel}: returned null key omitted: ${key}`);
        assert.equal(Object.hasOwn(persistedResult, key), true, `${rowLabel}: persisted null key omitted: ${key}`);
      }
    }
    for (const key of H12_NO_MONEY_PERSISTED_WRAPPER_KEYS) {
      assert.equal(Object.hasOwn(persistedResult, key), true, `${rowLabel}: persisted wrapper key missing: ${key}`);
    }
    assert.equal(Object.hasOwn(returnedResult, 'blocker'), true, `${rowLabel}: returned null blocker key omitted`);
    assert.equal(Object.hasOwn(persistedResult, 'blocker'), true, `${rowLabel}: persisted null blocker key omitted`);
    assert.equal(returnedResult.blocker, null, `${rowLabel}: returned blocker must be explicit JSON null`);
    assert.equal(persistedResult.blocker, null, `${rowLabel}: persisted blocker must be explicit JSON null`);
    assert.deepEqual(returnedResult, persistedContractResult, `${rowLabel}: returned/persisted result changed`);
    assert.equal(row.returned_persisted_contract_jsonb_equal, true, `${rowLabel}: jsonb equality failed`);
    assert.equal(
      row.returned_persisted_contract_canonical_text_equal,
      true,
      `${rowLabel}: canonical JSON text equality failed`
    );
    assert.equal(
      row.returned_result_sha256,
      row.persisted_contract_result_sha256,
      `${rowLabel}: returned/persisted result hash changed`
    );
  }
  return evidence;
}

function summarizeH12NoMoneyResultContract(evidence) {
  return {
    work_item_count: evidence.work_item_count,
    expected_top_level_result_key_count: H12_NO_MONEY_RESULT_KEYS.length,
    expected_full_persisted_key_count:
      H12_NO_MONEY_RESULT_KEYS.length + H12_NO_MONEY_PERSISTED_WRAPPER_KEYS.length,
    expected_persisted_wrapper_keys: [...H12_NO_MONEY_PERSISTED_WRAPPER_KEYS],
    returned_top_level_result_keys_complete: true,
    full_persisted_result_keys_complete: true,
    persisted_top_level_result_keys_complete: true,
    explicit_null_key_presence_verified: true,
    returned_persisted_contract_jsonb_equal: true,
    returned_persisted_contract_canonical_text_equal: true,
    returned_persisted_contract_sha256_equal: true
  };
}

function runH12Cancellation(target, channel, batchId) {
  const result = runBaseCancellation(target, channel, batchId);
  const requestId = result.h12_request_id;
  const operationId = result.h12_operation_id;
  assert.match(requestId, /^[0-9a-f-]{36}$/i);
  assert.match(operationId, /^[0-9a-f-]{36}$/i);
  const evidence = readH12Evidence(target, batchId, requestId, operationId);
  const expectedCandidateCount = result.normalized.cancellation_scope === 'ONE_CANDIDATE'
    ? 1
    : result.normalized.batch_candidate_count;
  const expectedAmbiguityCountPerCandidate = channel === 'PAYE' ? 3 : 2;
  assert.equal(evidence.request_status, 'APPLIED', JSON.stringify(evidence));
  assert.equal(evidence.operation_status, 'COMPLETE', JSON.stringify(evidence));
  assert.equal(evidence.candidate_scope_contract_version, 2, JSON.stringify(evidence));
  assert.equal(evidence.candidate_scope_hash_version, 2, JSON.stringify(evidence));
  assert.equal(evidence.source_row_count_semantics, 'FINANCIAL_ONLY', JSON.stringify(evidence));
  assert.equal(evidence.communication_cleanup_contract_version, 2, JSON.stringify(evidence));
  assert.equal(evidence.work_item_communication_contract_mismatch_count, 0, JSON.stringify(evidence));
  assert.equal(evidence.applied_result_communication_contract_mismatch_count, 0, JSON.stringify(evidence));
  assert.equal(evidence.selected_fixture_candidate_count, expectedCandidateCount, JSON.stringify(evidence));
  assert.equal(evidence.selected_safe_count, expectedCandidateCount, JSON.stringify(evidence));
  assert.equal(
    evidence.selected_ambiguous_count,
    expectedCandidateCount * expectedAmbiguityCountPerCandidate,
    JSON.stringify(evidence)
  );
  assert.equal(evidence.selected_ordinary_count, expectedCandidateCount, JSON.stringify(evidence));
  assert.equal(evidence.safe_carry_forward_count, expectedCandidateCount, JSON.stringify(evidence));
  assert.equal(evidence.ambiguous_carry_forward_count, 0, JSON.stringify(evidence));
  assert.equal(evidence.correction_evidence_count, evidence.selected_fixture_item_count, JSON.stringify(evidence));
  assert.equal(evidence.ambiguous_correction_evidence_count, evidence.selected_ambiguous_count, JSON.stringify(evidence));
  assert.equal(evidence.exact_before_snapshot_count, evidence.selected_fixture_item_count, JSON.stringify(evidence));
  assert.equal(evidence.ambiguous_workbench_reappearance_count, 0, JSON.stringify(evidence));
  assert.equal(evidence.safe_workbench_reappearance_count, evidence.selected_safe_count, JSON.stringify(evidence));
  assert.equal(evidence.ambiguous_alert_evidence_count, evidence.selected_ambiguous_count, JSON.stringify(evidence));
  assert.equal(evidence.ambiguous_alert_wrong_multiplicity_count, 0, JSON.stringify(evidence));
  assert.equal(evidence.alert_contains_sensitive_bank_key, false, JSON.stringify(evidence));
  assert.equal(evidence.manual_support_work_item_count, expectedCandidateCount, JSON.stringify(evidence));
  assert.equal(
    evidence.mail_fixture_count,
    result.normalized.batch_candidate_count * 4,
    JSON.stringify(evidence)
  );
  assert.equal(evidence.mail_fixture_kind_count, 4, JSON.stringify(evidence));
  assert.equal(evidence.mail_changed_or_missing_count, 0, JSON.stringify(evidence));
  assert.equal(evidence.mail_status_mismatch_count, 0, JSON.stringify(evidence));
  assert.equal(evidence.mail_lease_mismatch_count, 0, JSON.stringify(evidence));
  assert.equal(
    evidence.mail_claimed_fixture_count,
    result.normalized.batch_candidate_count,
    JSON.stringify(evidence)
  );
  assert.equal(
    evidence.mail_sent_fixture_count,
    result.normalized.batch_candidate_count,
    JSON.stringify(evidence)
  );
  const noMoneyResultContractBeforeExactReplay = result.h12_no_money_result_contract
    ? assertH12NoMoneyResultContract(
        readH12NoMoneyResultContractEvidence(target, evidence.request_id),
        expectedCandidateCount,
        'H12 no-money result before exact request replay'
      )
    : null;
  if (result.h12_no_money_result_contract) {
    assert.equal(result.h12_no_money_result_contract.work_item_count, expectedCandidateCount);
    assert.equal(result.h12_no_money_result_contract.expected_top_level_result_key_count, 82);
    assert.equal(result.h12_no_money_result_contract.expected_full_persisted_key_count, 85);
    assert.deepEqual(
      result.h12_no_money_result_contract.expected_persisted_wrapper_keys,
      ['candidate_scope_hash', 'created_by', 'selection_ordinal']
    );
    assert.equal(result.h12_no_money_result_contract.returned_top_level_result_keys_complete, true);
    assert.equal(result.h12_no_money_result_contract.full_persisted_result_keys_complete, true);
    assert.equal(result.h12_no_money_result_contract.persisted_top_level_result_keys_complete, true);
    assert.equal(result.h12_no_money_result_contract.explicit_null_key_presence_verified, true);
    assert.equal(result.h12_no_money_result_contract.returned_persisted_contract_jsonb_equal, true);
    assert.equal(result.h12_no_money_result_contract.returned_persisted_contract_canonical_text_equal, true);
    assert.equal(result.h12_no_money_result_contract.returned_persisted_contract_sha256_equal, true);
  }
  const replay = replayH12CompletedRequest(target, evidence.request_id, batchId);
  assert.equal(replay.ok, true, JSON.stringify(replay));
  assert.equal(replay.existing_request, true, JSON.stringify(replay));
  assert.equal(replay.correction_request_id, evidence.request_id, JSON.stringify(replay));
  assert.equal(replay.operation_id, evidence.operation_id, JSON.stringify(replay));
  const completedRequestReplayRoute = result.normalized.cancellation_scope === 'WHOLE_BATCH'
      && result.normalized.payment_state_before === 'DRAFT'
    ? 'WORKER_FIXED_DRAFT_CANCELLATION_LOOKUP'
    : 'PAYMENT_CORRECTION_REQUEST_START';
  if (completedRequestReplayRoute === 'WORKER_FIXED_DRAFT_CANCELLATION_LOOKUP') {
    assert.equal(replay.request_status, 'APPLIED', JSON.stringify(replay));
    assert.equal(replay.operation_status, 'COMPLETE', JSON.stringify(replay));
    assert.equal(replay.phase, 'COMPLETE', JSON.stringify(replay));
  }
  if (noMoneyResultContractBeforeExactReplay) {
    const noMoneyResultContractAfterExactReplay = assertH12NoMoneyResultContract(
      readH12NoMoneyResultContractEvidence(target, evidence.request_id),
      expectedCandidateCount,
      'H12 no-money result after exact request replay'
    );
    assert.deepEqual(
      noMoneyResultContractAfterExactReplay,
      noMoneyResultContractBeforeExactReplay,
      'H12_NO_MONEY_RESULT_EXACT_REQUEST_REPLAY_MISMATCH'
    );
    result.h12_no_money_result_contract.exact_request_replay_exact_equality = true;
  }
  const ownerBudgetedRpcs = new Set([
    'pay_payment_correction_request_start',
    'pay_payment_correction_reauth_bind_v1',
    'pay_payment_correction_process_chunk',
    'pay_batch_cancel'
  ]);
  for (const call of result.phase_calls) {
    if (ownerBudgetedRpcs.has(call.rpc)) {
      assert.ok(call.elapsed_ms < 6000, JSON.stringify(call));
    } else {
      assert.ok(call.elapsed_ms < 15000, JSON.stringify(call));
    }
  }
  result.h12_source_less = {
    ...evidence,
    completed_request_replay_same_request: true,
    completed_request_replay_same_operation: true,
    completed_request_replay_route: completedRequestReplayRoute,
    no_money_result_contract: result.h12_no_money_result_contract ?? null
  };
  delete result.h12_request_id;
  delete result.h12_operation_id;
  delete result.h12_no_money_result_contract;
  return result;
}

function buildTransformedRunner(engine, candidatePaths) {
  const sourceBuffer = fs.readFileSync(baseRunnerPath);
  assert.equal(sha256(sourceBuffer), BASE_RUNNER_SHA256, 'established cancellation runner changed');
  let source = sourceBuffer.toString('utf8').replaceAll('\r\n', '\n');
  const preparedSourceFingerprint = sha256(JSON.stringify({
    contract: 'H12_PRE_CANCELLATION_PREPARED_SOURCE_V1',
    mode: MODE,
    base_runner_sha256: BASE_RUNNER_SHA256,
    setup_sql_sha256: sha256(fs.readFileSync(setupSqlPath)),
    candidate_sources: candidatePaths.map((absolute) => ({
      path: path.relative(repoRoot, absolute).replaceAll('\\', '/'),
      sha256: sha256(fs.readFileSync(absolute))
    }))
  }));

  const injectedConstants = [
    `const H12_TEMPLATE_DATABASE = ${JSON.stringify(engine.templateDatabase)};`,
    `const H12_PREPARED_TEMPLATE_DATABASE = ${JSON.stringify(engine.preparedTemplateDatabase)};`,
    `const H12_PREPARED_SOURCE_FINGERPRINT = ${JSON.stringify(preparedSourceFingerprint)};`,
    `const H12_SETUP_SQL_PATH = ${JSON.stringify(setupSqlPath)};`,
    `const H12_CANDIDATE_SOURCE_PATHS = ${JSON.stringify(candidatePaths)};`,
    `const H12_SCOPE_INVALIDATOR_PAIR_ARRAYS_PATH = ${JSON.stringify(path.resolve(repoRoot, ...SCOPE_INVALIDATOR_PAIR_ARRAYS_PATH.split('/')))};`,
    `const H12_SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH = ${JSON.stringify(path.resolve(repoRoot, ...SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH.split('/')))};`,
    `const H12_NO_MONEY_RESULT_KEYS = Object.freeze(${JSON.stringify(sourceLessNoMoneyResultKeys)});`,
    `const H12_NO_MONEY_PERSISTED_WRAPPER_KEYS = Object.freeze(${JSON.stringify(SOURCE_LESS_NO_MONEY_PERSISTED_WRAPPER_KEYS)});`,
    'let h12LastAmbiguityActivation = null;'
  ].join('\n');
  const injectedFunctions = [
    installH12CandidateSources,
    installH12Fixture,
    establishH12ProductionShapedWorkbenchSource,
    establishH12CurrentWorkbenchAuthority,
    readH12PreparedSnapshotBoundary,
    prepareH12FreshPreCancellationBoundary,
    activateH12AmbiguityFixture,
    convergeH12FixtureCreatedWorkbenchJobs,
    startH12WholeNoMoneyCancellation,
    readH12Evidence,
    readH12WholeBatchCancellationFinancials,
    replayH12CompletedRequest,
    readH12PersistedContractEvidence,
    enableH12NoMoneyResultReturnCapture,
    readH12NoMoneyResultContractEvidence,
    assertH12NoMoneyResultContract,
    summarizeH12NoMoneyResultContract,
    runH12Cancellation
  ].map((fn) => fn.toString()).join('\n\n');

  source = replaceExactlyOnce(
    source,
    "const ACTOR_ID = '10000000-0000-4000-8000-000000000001';",
    `const ACTOR_ID = '10000000-0000-4000-8000-000000000001';\n${injectedConstants}`,
    'H12 constants'
  );
  const establishedTargetDatabase = engine.target === 'PG17_V8'
    ? 'h2_cancel_v8_pg17'
    : 'h2_cancel_v8_pg18';
  source = replaceExactlyOnce(
    source,
    `      database: '${establishedTargetDatabase}',`,
    `      database: ${JSON.stringify(engine.targetDatabase)},`,
    'H12 isolated target database binding'
  );
  source = replaceExactlyOnce(
    source,
    "  const original = fs.readFileSync(SCHEDULED_LOCAL_PREPARE_RUNTIME_SOURCE, 'utf8').replaceAll('\\r\\n', '\\n');",
    `  const scheduledFixtureSource = fs.readFileSync(SCHEDULED_LOCAL_PREPARE_RUNTIME_SOURCE, 'utf8').replaceAll('\\r\\n', '\\n');
  const scheduledFixtureWithFutureOperation = replaceExactlyOnce(
    scheduledFixtureSource,
    "      'payment_date', current_date::text,",
    "      'payment_date', (current_date + 1)::text,",
    'H12 disposable operation payment date'
  );
  const original = replaceExactlyOnce(
    scheduledFixtureWithFutureOperation,
    "    current_date,\\n    'ALL',",
    "    (current_date + 1),\\n    'ALL',",
    'H12 disposable authorisation payment date'
  );`,
    'H12 disposable future payment date'
  );

  const cloneStart = source.indexOf('function cloneDatabase(target) {');
  const cloneEnd = source.indexOf('\n\nfunction dropDatabase(target) {', cloneStart);
  assert.ok(cloneStart >= 0 && cloneEnd > cloneStart, 'cloneDatabase boundary changed');
  source = source.slice(0, cloneStart) + `function cloneDatabase(target) {
  assert.match(target.database, /^(?:h2_cancel_(?:v1|v8)|h12_rg5_builder_cancel)_pg(?:17|18)$/);
  const usePreparedSnapshot = String(
    process.env.H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT || ''
  ).trim().toLowerCase() === 'true';
  const verifyPreparedSnapshot = String(
    process.env.H12_SOURCE_LESS_VERIFY_PREPARED_SNAPSHOT || ''
  ).trim().toLowerCase() === 'true';
  const templateDatabase = usePreparedSnapshot || verifyPreparedSnapshot
    ? H12_PREPARED_TEMPLATE_DATABASE
    : H12_TEMPLATE_DATABASE;
  assert.match(H12_TEMPLATE_DATABASE, /^(?:h12_sourceless_baseline|h12_rg5_builder)_pg(?:17|18)$/);
  assert.match(H12_PREPARED_TEMPLATE_DATABASE, /^h12_rg5_prepared_pg(?:17|18)$/);
  runDocker(target.container, ['dropdb', '-U', 'postgres', '--if-exists', target.database]);
  runDocker(target.container, ['createdb', '-U', 'postgres', '-T', templateDatabase, target.database]);
}` + source.slice(cloneEnd);

  const dropStart = source.indexOf('function dropDatabase(target) {');
  const dropEnd = source.indexOf('\n\nfunction normalizeCancellationScaleFinancials(', dropStart);
  assert.ok(dropStart >= 0 && dropEnd > dropStart, 'dropDatabase boundary changed');
  source = source.slice(0, dropStart) + `function dropDatabase(target) {
  assert.match(target.database, /^(?:h2_cancel_(?:v1|v8)|h12_rg5_builder_cancel)_pg(?:17|18)$/);
  runDocker(target.container, ['dropdb', '-U', 'postgres', '--if-exists', target.database], { allowFailure: true });
}` + source.slice(dropEnd);

  source = replaceExactlyOnce(
    source,
    'function runCancellation(target, channel, batchId) {',
    'function runBaseCancellation(target, channel, batchId) {',
    'runCancellation rename'
  );
  source = replaceExactlyOnce(
    source,
    '    establishProductionShapedWorkbenchSource(target, batchId);',
    '    establishH12ProductionShapedWorkbenchSource(target, batchId);',
    'H12 complete Draft source shaping'
  );
  source = replaceExactlyOnce(
    source,
    '    ? establishCurrentWorkbenchAuthority(target, batchId)',
    '    ? establishH12CurrentWorkbenchAuthority(target, batchId)',
    'H12 complete Draft current authority'
  );
  source = replaceExactlyOnce(
    source,
    `  let productionShapeWorkbenchConvergence = null;
  let productionShapeComponentProof = null;
  if (String(process.env.H2_CANCEL_PRODUCTION_SHAPED_SOURCE || '').trim().toLowerCase() === 'true') {
    establishH12ProductionShapedWorkbenchSource(target, batchId);
    productionShapeWorkbenchConvergence = drainCancellationWorkbenchSourceBuilds(target, batchId);
    productionShapeComponentProof = assertProductionShapedWorkbenchComponents(target, batchId);
  }
  const establishedCurrentness = String(process.env.H2_CANCEL_ESTABLISH_CURRENT_SOURCE || '').trim().toLowerCase() === 'true'
    ? establishH12CurrentWorkbenchAuthority(target, batchId)
    : null;
  const certifiedBaselineWorkbenchConvergence = establishedCurrentness
    ? drainCancellationWorkbenchSourceBuilds(target, batchId)
    : null;
  const currentnessBefore = certifiedBaselineWorkbenchConvergence?.currentness
    || productionShapeWorkbenchConvergence?.currentness
    || establishedCurrentness;
  const preScheduleWorkbenchInvariant = assertCancellationWorkbenchPreScheduleInvariant(target, batchId);`,
    `  const h12UsePreparedSnapshot = String(
    process.env.H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT || ''
  ).trim().toLowerCase() === 'true';
  let productionShapeWorkbenchConvergence = null;
  let productionShapeComponentProof = null;
  let establishedCurrentness = null;
  let certifiedBaselineWorkbenchConvergence = null;
  let currentnessBefore = null;
  let preScheduleWorkbenchInvariant = null;
  if (h12UsePreparedSnapshot) {
    // This branch is read-only. The clone was proved byte-for-byte equivalent
    // at the semantic boundary before any route, fixture or scope mutation.
    productionShapeComponentProof = assertProductionShapedWorkbenchComponents(target, batchId);
    preScheduleWorkbenchInvariant = assertCancellationWorkbenchPreScheduleInvariant(target, batchId);
    currentnessBefore = preScheduleWorkbenchInvariant.currentness;
  } else {
    if (String(process.env.H2_CANCEL_PRODUCTION_SHAPED_SOURCE || '').trim().toLowerCase() === 'true') {
      establishH12ProductionShapedWorkbenchSource(target, batchId);
      productionShapeWorkbenchConvergence = drainCancellationWorkbenchSourceBuilds(target, batchId);
      productionShapeComponentProof = assertProductionShapedWorkbenchComponents(target, batchId);
    }
    establishedCurrentness = String(process.env.H2_CANCEL_ESTABLISH_CURRENT_SOURCE || '').trim().toLowerCase() === 'true'
      ? establishH12CurrentWorkbenchAuthority(target, batchId)
      : null;
    certifiedBaselineWorkbenchConvergence = establishedCurrentness
      ? drainCancellationWorkbenchSourceBuilds(target, batchId)
      : null;
    currentnessBefore = certifiedBaselineWorkbenchConvergence?.currentness
      || productionShapeWorkbenchConvergence?.currentness
      || establishedCurrentness;
    preScheduleWorkbenchInvariant = assertCancellationWorkbenchPreScheduleInvariant(target, batchId);
  }
  if (String(
    process.env.H12_SOURCE_LESS_CAPTURE_PRE_CANCELLATION_BOUNDARY || ''
  ).trim().toLowerCase() === 'true') {
    assert.equal(h12UsePreparedSnapshot, false, 'prepared snapshots cannot prepare themselves');
    return {
      h12_pre_cancellation_boundary_only: true,
      currentness_before: currentnessBefore,
      production_shape_component_proof: productionShapeComponentProof,
      pre_schedule_workbench_invariant: preScheduleWorkbenchInvariant,
      route_mutation_started: false
    };
  }`,
    'H12 immutable pre-cancellation snapshot boundary'
  );
  source = replaceExactlyOnce(
    source,
    "  if (paymentState !== 'DRAFT') assert.equal(cancellationScope, 'ONE_CANDIDATE');\n",
    '',
    'whole no-money admission'
  );
  source = replaceExactlyOnce(
    source,
    "      ? 'FAILED_PAYMENT_CONFIRMED_NO_MONEY_RELEASED_BY_USER'",
    "      ? 'FAILED_PAYMENT_RELEASE_CONFIRMED_NOT_PAID'",
    'Worker-owned no-money correction reason'
  );
  source = replaceExactlyOnce(
    source,
    "      : null);\n  const before = queryJson(target, `",
    `      : null);
  installH12Fixture(target);
  h12LastAmbiguityActivation = String(
    process.env.H12_SOURCE_LESS_FIXTURE_MODE || 'FULL'
  ).trim().toUpperCase() === 'FULL'
    ? activateH12AmbiguityFixture(target)
    : null;
  // The H12 fixture inserts frozen historical Draft rows and alters only their
  // deliberately malformed evidence. Those writes fire the real dirty
  // triggers, so process the resulting durable work rather than deleting it.
  // The complete batch normal lane must reach quiescence before any source
  // claim, matching the production Worker scheduling contract.
  const h12FixtureWorkbenchConvergence = h12LastAmbiguityActivation
    ? convergeH12FixtureCreatedWorkbenchJobs(target, batchId)
    : null;
  const before = queryJson(target, \``,
    'activate ambiguity only after payment-state preparation'
  );

  const oldWholeStart = `  } else {
    started = queryJson(target, \`
      SELECT public.pay_batch_cancel(
        p_pay_batch_id := '\${batchId}'::uuid,
        p_actor_user_id := '\${ACTOR_ID}'::uuid,
        p_reason := 'DRAFT_PAYMENT_CANCELLED_BY_USER',
        p_correction_request_id := NULL::uuid,
        p_work_item_id := NULL::uuid
      )::text
    \`);
  }`;
  const newWholeStart = `  } else {
    if (terminalNoMoney) {
      started = startH12WholeNoMoneyCancellation(
        target,
        batchId,
        requestedAction,
        correctionReason,
        channel
      );
    } else {
      started = queryJson(target, \`
        SELECT public.pay_batch_cancel(
          p_pay_batch_id := '\${batchId}'::uuid,
          p_actor_user_id := '\${ACTOR_ID}'::uuid,
          p_reason := 'DRAFT_PAYMENT_CANCELLED_BY_USER',
          p_correction_request_id := NULL::uuid,
          p_work_item_id := NULL::uuid
        )::text
      \`);
    }
  }`;
  source = replaceExactlyOnce(source, oldWholeStart, newWholeStart, 'whole no-money request start');

  source = replaceExactlyOnce(
    source,
    `  if (cancellationScope === 'WHOLE_BATCH') {
    assert.ok(after.cancellation_audit_count >= 1);
  } else {
    // The established no-money owner writes its durable audit evidence to the
    // correction work item. Its separate _imp_debug_audit call is deliberately
    // controlled by invoice_debug and is not a permanent audit-row contract.
    assert.equal(after.correction_work_item_result_evidence_count, after.applied_work_item_count);
  }`,
    `  if (terminalNoMoney) {
    // NO_MONEY_RELEASE is completed by the correction owner for both scopes.
    // Its durable evidence is the persisted, exact work-item result; the
    // optional debug audit row is not a business contract.
    assert.equal(after.correction_work_item_result_evidence_count, after.applied_work_item_count);
  } else if (cancellationScope === 'WHOLE_BATCH') {
    assert.ok(after.cancellation_audit_count >= 1);
  } else {
    assert.equal(after.correction_work_item_result_evidence_count, after.applied_work_item_count);
  }`,
    'no-money durable result evidence for both cancellation scopes'
  );

  source = replaceExactlyOnce(
    source,
    `    cancellation_audit_present: cancellationScope === 'WHOLE_BATCH'
      ? after.cancellation_audit_count >= 1
      : after.correction_work_item_result_evidence_count === after.applied_work_item_count,`,
    `    cancellation_audit_present: terminalNoMoney
      ? after.correction_work_item_result_evidence_count === after.applied_work_item_count
      : (cancellationScope === 'WHOLE_BATCH'
        ? after.cancellation_audit_count >= 1
        : after.correction_work_item_result_evidence_count === after.applied_work_item_count),`,
    'no-money normalized durable result evidence'
  );

  source = replaceExactlyOnce(
    source,
    `  let prepareResponseLossReplay = null;
  if (simulateResponseLoss && cancellationScope === 'ONE_CANDIDATE') {`,
    `  let prepareResponseLossReplay = null;
  if (simulateResponseLoss && cancellationScope === 'WHOLE_BATCH') {
    const wholeReplayStartedAt = performance.now();
    const replayedStart = paymentState === 'DRAFT'
      ? {
          value: replayH12CompletedRequest(target, requestId, batchId),
          elapsedMs: Number((performance.now() - wholeReplayStartedAt).toFixed(3))
        }
      : startH12WholeNoMoneyCancellation(
          target,
          batchId,
          requestedAction,
          correctionReason,
          channel
        );
    phaseCalls.push({
      rpc: paymentState === 'DRAFT'
        ? 'worker_read_exact_draft_cancellation_replay_v1'
        : 'pay_payment_correction_request_start',
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
      existing_request: true,
      route: paymentState === 'DRAFT'
        ? 'WORKER_FIXED_DRAFT_CANCELLATION_LOOKUP'
        : 'WORKER_ALL_MATCHING_PAYMENT_STATUS_REQUEST_START'
    };
  }
  if (simulateResponseLoss && cancellationScope === 'ONE_CANDIDATE') {`,
    'whole-scope PREPARE response-loss replay'
  );

  source = replaceExactlyOnce(
    source,
    '  const prepareClaim = claim(target, operationId, workerId);',
    `  const h12PersistedContractUnderTest = String(
    process.env.H12_SOURCE_LESS_PERSISTED_CONTRACT || ''
  ).trim().toUpperCase();
  assert.ok(['', 'LEGACY_V1', 'LEGACY_V2_COMMUNICATION_V1'].includes(h12PersistedContractUnderTest));
  if (h12PersistedContractUnderTest === 'LEGACY_V1') {
    queryJson(target, \`
      UPDATE public.pay_payment_correction_requests AS legacy_request
      SET plan_json = (
        COALESCE(legacy_request.plan_json, '{}'::jsonb)
          - 'communication_cleanup_contract_version'
      ) || pg_catalog.jsonb_build_object(
        'candidate_scope_contract_version', 1,
        'candidate_scope_hash_version', 1,
        'source_row_count_semantics', 'FINANCIAL_AND_QUEUED_COMMUNICATIONS'
      )
      WHERE legacy_request.id = '\${requestId}'::uuid
      RETURNING pg_catalog.jsonb_build_object('ok', true)::text
    \`);
  }

  const prepareClaim = claim(target, operationId, workerId);
  const h12CompetingClaim = claim(target, operationId, workerId + '-competing');
  assert.equal(h12CompetingClaim.value.claimed, false, JSON.stringify(h12CompetingClaim.value));`,
    'competing operation claim'
  );
  source = replaceExactlyOnce(
    source,
    'function runTarget(target) {\n  cloneDatabase(target);\n  try {',
    `function runTarget(target) {
  cloneDatabase(target);
  try {
    const h12UsePreparedSnapshot = String(
      process.env.H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT || ''
    ).trim().toLowerCase() === 'true';
    const h12VerifyPreparedSnapshot = String(
      process.env.H12_SOURCE_LESS_VERIFY_PREPARED_SNAPSHOT || ''
    ).trim().toLowerCase() === 'true';
    assert.equal(
      h12UsePreparedSnapshot && h12VerifyPreparedSnapshot,
      false,
      'prepared snapshot use and verification modes are mutually exclusive'
    );
    let h12PreparedSnapshotClone = null;
    let scaleFinancialNormalization = { prepared_snapshot_reused: true };
    let diagnosticWrapperInstalled = false;
    if (h12UsePreparedSnapshot || h12VerifyPreparedSnapshot) {
      h12PreparedSnapshotClone = readH12PreparedSnapshotBoundary(target);
      if (h12VerifyPreparedSnapshot) {
        return { _h12_prepared_snapshot_clone: h12PreparedSnapshotClone };
      }
    }
    if (!h12UsePreparedSnapshot) {`,
    'H12 runTarget boundary'
  );
  source = replaceExactlyOnce(
    source,
    '    const scaleFinancialNormalization = normalizeCancellationScaleFinancials(target);',
    '    scaleFinancialNormalization = normalizeCancellationScaleFinancials(target);',
    'H12 prepared snapshot normalization bypass'
  );
  source = replaceExactlyOnce(
    source,
    "    const diagnosticWrapperInstalled = String(process.env.H2_CANCEL_INSTRUMENT_REFRESH || '').trim().toLowerCase() === 'true';",
    "    diagnosticWrapperInstalled = String(process.env.H2_CANCEL_INSTRUMENT_REFRESH || '').trim().toLowerCase() === 'true';",
    'H12 prepared snapshot diagnostic binding'
  );
  source = replaceExactlyOnce(
    source,
    '    installRepositoryReconciliationEnvelope(target);',
    `    installRepositoryReconciliationEnvelope(target);
    h12LastAmbiguityActivation = null;`,
    'H12 post-envelope setup call'
  );
  source = replaceExactlyOnce(
    source,
    '    const setPageEnabled = String(process.env.H2_CANCEL_SET_PAGE || \'\').trim().toLowerCase() === \'true\';',
    `    if (String(process.env.H12_SOURCE_LESS_DEFER_CANDIDATE_INSTALL || '').trim().toLowerCase() !== 'true') {
      // The disposable V8 snapshot first receives every already-settled
      // baseline correction above.  Install the H12 successors last so an
      // older baseline closure cannot silently overwrite their exact owners.
      installH12CandidateSources(target);
    }
    const setPageEnabled = String(process.env.H2_CANCEL_SET_PAGE || '').trim().toLowerCase() === 'true';`,
    'H12 post-current-baseline candidate install order'
  );
  source = replaceExactlyOnce(
    source,
    "    const requestedChannel = String(process.env.H2_CANCEL_CHANNEL || '').trim().toUpperCase();",
    `    }
    const requestedChannel = String(process.env.H2_CANCEL_CHANNEL || '').trim().toUpperCase();`,
    'H12 prepared snapshot common-setup boundary'
  );
  source = replaceExactlyOnce(
    source,
    `    const preexistingWorkbenchConvergence = Object.fromEntries(
      channels.map((channel) => [
        channel,
        drainCancellationWorkbenchSourceBuilds(target, target.batches[channel])
      ])
    );
    const scheduledPreparation = {};`,
    `    const preexistingWorkbenchConvergence = h12UsePreparedSnapshot
      ? Object.fromEntries(channels.map((channel) => [
          channel,
          {
            currentness: h12PreparedSnapshotClone.semantic_manifest.channels[channel].invariant.currentness,
            pre_schedule_invariant: h12PreparedSnapshotClone.semantic_manifest.channels[channel].invariant,
            safe_steps: [],
            iteration_count: 0,
            prepared_snapshot_reused_without_mutation: true
          }
        ]))
      : Object.fromEntries(
          channels.map((channel) => [
            channel,
            drainCancellationWorkbenchSourceBuilds(target, target.batches[channel])
          ])
        );
    if (String(
      process.env.H12_SOURCE_LESS_PREPARE_SNAPSHOT || ''
    ).trim().toLowerCase() === 'true') {
      assert.equal(h12UsePreparedSnapshot, false);
      assert.equal(h12VerifyPreparedSnapshot, false);
      assert.equal(requestedChannel, '', 'snapshot preparation must cover both channels');
      return {
        _h12_prepared_snapshot_boundary: prepareH12FreshPreCancellationBoundary(target)
      };
    }
    const scheduledPreparation = {};`,
    'H12 prepared snapshot convergence reuse and preparation mode'
  );
  source = replaceExactlyOnce(
    source,
    '    const results = {\n      _fixture_convergence:',
    `    const results = {
      _h12_prepared_snapshot_clone: h12PreparedSnapshotClone,
      _fixture_convergence:`,
    'H12 activation result evidence'
  );
  source = replaceExactlyOnce(
    source,
    '    return results;\n  } finally {',
    `    results._h12_ambiguity_activation = h12LastAmbiguityActivation;
    return results;
  } finally {`,
    'H12 post-payment-state activation evidence'
  );
  source = replaceExactlyOnce(
    source,
    'Object.entries(targetResults).map(([channel, result]) => [channel, result.normalized',
    "Object.entries(targetResults).map(([channel, result]) => [channel, channel.startsWith('_') ? result : result.normalized",
    'H12 compact snapshot control evidence preservation'
  );
  source = replaceExactlyOnce(
    source,
    '\nfunction runTarget(target) {',
    `\n${injectedFunctions}\n\nfunction runTarget(target) {`,
    'H12 support function injection'
  );
  source = replaceExactlyOnce(
    source,
    'function drainCancellationWorkbenchSourceBuilds(target, batchId) {\n  const scope = queryJson(target, `',
    'function drainCancellationWorkbenchSourceBuilds(target, batchId) {\n  const h12DrainStartedAt = performance.now();\n  const scope = queryJson(target, `',
    'H12 Workbench drain elapsed-time start'
  );
  source = replaceExactlyOnce(
    source,
    '        const normalDrain = queryJson(target, `',
    '        const normalDrainCall = queryJson(target, `',
    'H12 normal-drain call timing binding'
  );
  source = replaceExactlyOnce(
    source,
    '        `).value;\n        assert.equal(normalDrain.ok, true, JSON.stringify({',
    '        `);\n        const normalDrain = normalDrainCall.value;\n        assert.equal(normalDrain.ok, true, JSON.stringify({',
    'H12 normal-drain call result binding'
  );
  source = replaceExactlyOnce(
    source,
    "          result_code: normalDrain.stop_reason || null\n        });",
    "          result_code: normalDrain.stop_reason || null,\n          elapsed_ms: normalDrainCall.elapsedMs\n        });",
    'H12 normal-drain call timing evidence'
  );
  source = replaceExactlyOnce(
    source,
    '      const claim = queryJson(target, `',
    '      const claimCall = queryJson(target, `',
    'H12 source-build claim timing binding'
  );
  source = replaceExactlyOnce(
    source,
    '      `).value;\n      assert.equal(claim.ok, true, JSON.stringify(claim));\n      if (claim.claimed !== true) {',
    '      `);\n      const claim = claimCall.value;\n      assert.equal(claim.ok, true, JSON.stringify(claim));\n      if (claim.claimed !== true) {',
    'H12 source-build claim result binding'
  );
  source = replaceExactlyOnce(
    source,
    "            continuation_enqueued: false\n          });",
    "            continuation_enqueued: false,\n            elapsed_ms: claimCall.elapsedMs\n          });",
    'H12 source-build scan timing evidence'
  );
  source = replaceExactlyOnce(
    source,
    '      const execution = queryJson(target, `',
    '      const executionCall = queryJson(target, `',
    'H12 source-build execution timing binding'
  );
  source = replaceExactlyOnce(
    source,
    '      `).value;\n      const failureEvidence = execution.ok === true ? null : queryJson(target, `',
    '      `);\n      const execution = executionCall.value;\n      const failureEvidence = execution.ok === true ? null : queryJson(target, `',
    'H12 source-build execution result binding'
  );
  source = replaceExactlyOnce(
    source,
    "        failure_evidence: failureEvidence\n      });",
    "        failure_evidence: failureEvidence,\n        claim_elapsed_ms: claimCall.elapsedMs,\n        execute_elapsed_ms: executionCall.elapsedMs\n      });",
    'H12 source-build call timing evidence'
  );
  source = replaceExactlyOnce(
    source,
    '        iteration_count: iteration\n      };',
    `        iteration_count: iteration,
        elapsed_ms: Number((performance.now() - h12DrainStartedAt).toFixed(3))
      };`,
    'H12 Workbench drain success metrics'
  );
  source = replaceExactlyOnce(
    source,
    'results[channel] = runCancellation(target, channel, target.batches[channel]);',
    `results[channel] = String(process.env.H12_SOURCE_LESS_FIXTURE_MODE || 'FULL').trim().toUpperCase() === 'MAIL_ONLY'
          ? runBaseCancellation(target, channel, target.batches[channel])
          : runH12Cancellation(target, channel, target.batches[channel]);`,
    'H12 cancellation wrapper call'
  );
  source = replaceExactlyOnce(
    source,
    "          AND work_item_row.result_json->>'correction_item_kind' = 'NO_MONEY_UNWIND'",
    "          AND work_item_row.result_json->>'correction_item_kind' = '${terminalNoMoney ? 'NO_MONEY_UNWIND' : 'PRE_BANK_CANCEL'}'",
    'H12 route-specific durable work-item evidence kind'
  );
  source = replaceExactlyOnce(
    source,
    "  const qBoundCancellationAuthority = paymentState === 'SCHEDULED_LOCAL_NOT_SENT'",
    `  const h12DraftCancelSelectionIdentityBefore = queryJson(target, \`
        WITH cancelled_candidate_scope AS (
          SELECT candidate_row.candidate_id
          FROM public.pay_batch_candidates AS candidate_row
          WHERE candidate_row.pay_batch_id = '\${batchId}'::uuid
            AND (
              '\${cancellationScope}' <> 'ONE_CANDIDATE'
              OR candidate_row.id = '\${selectedCandidate?.candidate_token || '00000000-0000-0000-0000-000000000000'}'::uuid
            )
        ), frozen_selection_keys AS (
          SELECT DISTINCT
            frozen_payload.candidate_id,
            public._pay_workbench_preview_selection_key_v1(
              frozen_payload.candidate_id,
              frozen_payload.payload_json->>'section',
              frozen_payload.timesheet_id,
              frozen_payload.payload_json->>'key_type',
              frozen_payload.payload_json->>'key_value',
              frozen_payload.row_key,
              frozen_payload.payload_json
            ) AS stable_selection_key
          FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
          JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS frozen_payload
            ON frozen_payload.operation_id = frozen_scope.operation_id
           AND frozen_payload.candidate_id = frozen_scope.candidate_id
           AND frozen_payload.resolved_pay_channel = frozen_scope.resolved_pay_channel
          WHERE frozen_scope.pay_batch_id = '\${batchId}'::uuid
            AND frozen_scope.candidate_id IN (SELECT candidate_id FROM cancelled_candidate_scope)
        ), unrelated_selected_rows AS (
          SELECT
            preview_row.candidate_id,
            preview_row.row_json->>'selection_identity_digest' AS selection_identity_digest,
            public._pay_workbench_preview_selection_key_v1(
              preview_row.candidate_id,
              preview_row.section,
              preview_row.timesheet_id,
              preview_row.key_type,
              preview_row.key_value,
              preview_row.row_key,
              preview_row.row_json
            ) AS stable_selection_key
          FROM public.banking_pay_workbench_preview_rows AS preview_row
          JOIN public.pay_batches AS batch_row
            ON batch_row.source_workbench_session_id = preview_row.session_id
           AND batch_row.id = '\${batchId}'::uuid
          WHERE preview_row.candidate_id IN (SELECT candidate_id FROM cancelled_candidate_scope)
            AND preview_row.status = 'READY'
            AND preview_row.selected IS TRUE
            AND preview_row.selection_state = 'SELECTED'
            AND NOT EXISTS (
              SELECT 1
              FROM frozen_selection_keys AS frozen_key
              WHERE frozen_key.candidate_id = preview_row.candidate_id
                AND frozen_key.stable_selection_key = public._pay_workbench_preview_selection_key_v1(
                  preview_row.candidate_id,
                  preview_row.section,
                  preview_row.timesheet_id,
                  preview_row.key_type,
                  preview_row.key_value,
                  preview_row.row_key,
                  preview_row.row_json
                )
            )
        )
        SELECT pg_catalog.jsonb_build_object(
          'cancelled_stable_keys', (
            SELECT COALESCE(pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'candidate_id', frozen_key.candidate_id,
                'stable_selection_key', frozen_key.stable_selection_key
              ) ORDER BY frozen_key.candidate_id, frozen_key.stable_selection_key
            ), '[]'::jsonb)
            FROM frozen_selection_keys AS frozen_key
            WHERE frozen_key.stable_selection_key IS NOT NULL
          ),
          'unrelated_selected_rows', (
            SELECT COALESCE(pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'candidate_id', unrelated_row.candidate_id,
                'selection_identity_digest', unrelated_row.selection_identity_digest,
                'stable_selection_key', unrelated_row.stable_selection_key
              ) ORDER BY unrelated_row.candidate_id, unrelated_row.stable_selection_key
            ), '[]'::jsonb)
            FROM unrelated_selected_rows AS unrelated_row
          )
        )::text
      \`).value;
  assert.ok(h12DraftCancelSelectionIdentityBefore.cancelled_stable_keys.length > 0);
  if (cancellationScope === 'ONE_CANDIDATE') {
    assert.ok(
      h12DraftCancelSelectionIdentityBefore.unrelated_selected_rows.length > 0,
      'H12_ONE_CANDIDATE_UNRELATED_SELECTED_CONTROL_MISSING'
    );
  }
  for (const expectedRow of h12DraftCancelSelectionIdentityBefore.cancelled_stable_keys) {
    assert.match(expectedRow.candidate_id, /^[0-9a-f-]{36}$/i);
    assert.ok(expectedRow.stable_selection_key.length > 0);
  }
  for (const expectedRow of h12DraftCancelSelectionIdentityBefore.unrelated_selected_rows) {
    assert.match(expectedRow.candidate_id, /^[0-9a-f-]{36}$/i);
    assert.match(expectedRow.selection_identity_digest, /^[0-9a-f]{64}$/i);
    assert.ok(expectedRow.stable_selection_key.length > 0);
  }

  const qBoundCancellationAuthority = paymentState === 'SCHEDULED_LOCAL_NOT_SENT'`,
    'H12 exact pre-apply Draft cancellation stable constituent and unrelated selection identity'
  );
  source = replaceExactlyOnce(
    source,
    "  if (cancellationScope === 'ONE_CANDIDATE') {\n    const selectedCandidateFinancialsAfter = readCandidateCancellationFinancials(",
    `  const h12DraftCancelSelectionIdentityAfter = workbenchDrain
    ? queryJson(target, \`
        WITH expected_cancelled AS (
          SELECT *
          FROM pg_catalog.jsonb_to_recordset(
            '\${JSON.stringify(h12DraftCancelSelectionIdentityBefore.cancelled_stable_keys).replaceAll("'", "''")}'::jsonb
          ) AS expected_row(candidate_id uuid, stable_selection_key text)
        ), expected_unrelated AS (
          SELECT *
          FROM pg_catalog.jsonb_to_recordset(
            '\${JSON.stringify(h12DraftCancelSelectionIdentityBefore.unrelated_selected_rows).replaceAll("'", "''")}'::jsonb
          ) AS expected_row(
            candidate_id uuid,
            selection_identity_digest text,
            stable_selection_key text
          )
        ), current_rows AS (
          SELECT
            preview_row.*,
            public._pay_workbench_preview_selection_key_v1(
              preview_row.candidate_id,
              preview_row.section,
              preview_row.timesheet_id,
              preview_row.key_type,
              preview_row.key_value,
              preview_row.row_key,
              preview_row.row_json
            ) AS stable_selection_key
          FROM public.banking_pay_workbench_preview_rows AS preview_row
          JOIN public.pay_batches AS batch_row
            ON batch_row.source_workbench_session_id = preview_row.session_id
           AND batch_row.id = '\${batchId}'::uuid
        )
        SELECT pg_catalog.jsonb_build_object(
          'cancelled_stable_keys', (
            SELECT COALESCE(pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'candidate_id', current_row.candidate_id,
                'stable_selection_key', current_row.stable_selection_key
              ) ORDER BY current_row.candidate_id, current_row.stable_selection_key
            ), '[]'::jsonb)
            FROM current_rows AS current_row
            JOIN expected_cancelled AS expected_row
              ON expected_row.candidate_id = current_row.candidate_id
             AND expected_row.stable_selection_key = current_row.stable_selection_key
            WHERE current_row.status = 'READY'
              AND current_row.selected IS FALSE
              AND current_row.selection_state = 'UNSELECTED'
              AND current_row.row_json->>'selection_user_override' = 'UNSELECTED'
          ),
          'unrelated_selected_rows', (
            SELECT COALESCE(pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'candidate_id', current_row.candidate_id,
                'selection_identity_digest', current_row.row_json->>'selection_identity_digest',
                'stable_selection_key', current_row.stable_selection_key
              ) ORDER BY current_row.candidate_id, current_row.stable_selection_key
            ), '[]'::jsonb)
            FROM current_rows AS current_row
            JOIN expected_unrelated AS expected_row
              ON expected_row.candidate_id = current_row.candidate_id
             AND expected_row.selection_identity_digest = current_row.row_json->>'selection_identity_digest'
             AND expected_row.stable_selection_key = current_row.stable_selection_key
            WHERE current_row.status = 'READY'
              AND current_row.selected IS TRUE
              AND current_row.selection_state = 'SELECTED'
          )
        )::text
      \`).value
    : { cancelled_stable_keys: [], unrelated_selected_rows: [] };
  assert.deepEqual(
    h12DraftCancelSelectionIdentityAfter.cancelled_stable_keys,
    h12DraftCancelSelectionIdentityBefore.cancelled_stable_keys,
    'H12_DRAFT_CANCEL_STABLE_CONSTITUENT_NOT_RETURNED_UNSELECTED'
  );
  assert.deepEqual(
    h12DraftCancelSelectionIdentityAfter.unrelated_selected_rows,
    h12DraftCancelSelectionIdentityBefore.unrelated_selected_rows,
    'H12_DRAFT_CANCEL_UNRELATED_READY_SELECTION_CHANGED'
  );

  if (cancellationScope === 'ONE_CANDIDATE') {
    const selectedCandidateFinancialsAfter = readCandidateCancellationFinancials(`,
    'H12 exact post-drain Draft cancellation stable constituent and unrelated selection identity'
  );
  source = replaceExactlyOnce(
    source,
    `  let processResponseLossReplay = null;\n  let iteration = 0;\n  while (operation.status !== 'COMPLETE' && operation.phase !== 'COMPLETE') {`,
    `  let processResponseLossReplay = null;
  let h12ContractCutover = null;
  let h12DeferredCandidateInstalled = false;
  let h12NoMoneyResultReturnCaptureEnabled = false;
  let h12NoMoneyResultContract = null;
  let iteration = 0;
  while (operation.status !== 'COMPLETE' && operation.phase !== 'COMPLETE') {
    if (String(process.env.H12_SOURCE_LESS_DEFER_CANDIDATE_INSTALL || '').trim().toLowerCase() === 'true'
        && h12DeferredCandidateInstalled === false
        && operation.phase === 'PROCESS_CHUNKS') {
      const beforeInstallContract = readH12PersistedContractEvidence(target, requestId);
      const expectedContract = h12PersistedContractUnderTest === 'LEGACY_V1'
        ? {
            candidate_scope_contract_version: 1,
            candidate_scope_hash_version: 1,
            source_row_count_semantics: 'FINANCIAL_AND_QUEUED_COMMUNICATIONS',
            communication_cleanup_contract_version: null
          }
        : {
            candidate_scope_contract_version: 2,
            candidate_scope_hash_version: 2,
            source_row_count_semantics: 'FINANCIAL_ONLY',
            communication_cleanup_contract_version: 1
          };
      assert.deepEqual(beforeInstallContract.request_contract, expectedContract);
      assert.deepEqual(beforeInstallContract.work_item_contracts, [expectedContract]);
      installH12CandidateSources(target);
      const afterInstallContract = readH12PersistedContractEvidence(target, requestId);
      assert.deepEqual(afterInstallContract, beforeInstallContract);
      h12ContractCutover = {
        case_id: 'NONTERMINAL_OLD_REQUEST_DETECTED_NOT_REINTERPRETED',
        persisted_contract: h12PersistedContractUnderTest,
        contract_before_install: beforeInstallContract,
        contract_after_install: afterInstallContract,
        exact_marker_preserved: true,
        deployment_date_fallback_used: false,
        release_safety_evidence_only: true,
        ui_retry_claim: false
      };
      h12DeferredCandidateInstalled = true;
    }
    if (terminalNoMoney
        && operation.phase === 'PROCESS_CHUNKS'
        && h12NoMoneyResultReturnCaptureEnabled === false) {
      enableH12NoMoneyResultReturnCapture(target);
      h12NoMoneyResultReturnCaptureEnabled = true;
    }`,
    'nonterminal old-contract cutover guard'
  );
  source = replaceExactlyOnce(
    source,
    `    if (simulateResponseLoss
        && cancellationScope === 'ONE_CANDIDATE'
        && operation.phase === 'PROCESS_CHUNKS'
        && processResponseLossReplay == null) {
      const effectAfterCommittedResponse = readCandidateCancellationFinancials(
        target,
        selectedCandidate.candidate_token
      );`,
    `    let h12NoMoneyResultBeforeResponseLoss = null;
    if (terminalNoMoney && operation.phase === 'PROCESS_CHUNKS') {
      const h12ExpectedNoMoneyWorkItemCount = cancellationScope === 'ONE_CANDIDATE'
        ? 1
        : before.batch_candidate_count;
      h12NoMoneyResultBeforeResponseLoss = assertH12NoMoneyResultContract(
        readH12NoMoneyResultContractEvidence(target, requestId),
        h12ExpectedNoMoneyWorkItemCount,
        'H12 no-money result after first committed process response'
      );
      h12NoMoneyResultContract = {
        ...summarizeH12NoMoneyResultContract(h12NoMoneyResultBeforeResponseLoss),
        process_response_loss_replay_exact_equality: null,
        exact_request_replay_exact_equality: null
      };
    }
    if (simulateResponseLoss
        && operation.phase === 'PROCESS_CHUNKS'
        && processResponseLossReplay == null) {
      const effectAfterCommittedResponse = cancellationScope === 'ONE_CANDIDATE'
        ? readCandidateCancellationFinancials(target, selectedCandidate.candidate_token)
        : readH12WholeBatchCancellationFinancials(target, batchId, requestId);`,
    'generic process response-loss pre-replay snapshot'
  );
  source = replaceExactlyOnce(
    source,
    `      const effectAfterReplay = readCandidateCancellationFinancials(
        target,
        selectedCandidate.candidate_token
      );
      assert.deepEqual(effectAfterReplay, effectAfterCommittedResponse);
      processResponseLossReplay = {
        persisted_phase_after_first_response: advanced.value.phase,
        resumed_phase: replayedProcess.value.phase,
        candidate_financial_effect_repeated: false,
        exact_candidate_financials_preserved: true
      };`,
    `      const effectAfterReplay = cancellationScope === 'ONE_CANDIDATE'
        ? readCandidateCancellationFinancials(target, selectedCandidate.candidate_token)
        : readH12WholeBatchCancellationFinancials(target, batchId, requestId);
      assert.deepEqual(effectAfterReplay, effectAfterCommittedResponse);
      if (terminalNoMoney) {
        const h12ExpectedNoMoneyWorkItemCount = cancellationScope === 'ONE_CANDIDATE'
          ? 1
          : before.batch_candidate_count;
        const h12NoMoneyResultAfterResponseLoss = assertH12NoMoneyResultContract(
          readH12NoMoneyResultContractEvidence(target, requestId),
          h12ExpectedNoMoneyWorkItemCount,
          'H12 no-money result after process response-loss replay'
        );
        assert.deepEqual(
          h12NoMoneyResultAfterResponseLoss,
          h12NoMoneyResultBeforeResponseLoss,
          'H12_NO_MONEY_RESULT_PROCESS_RESPONSE_LOSS_REPLAY_MISMATCH'
        );
        h12NoMoneyResultContract.process_response_loss_replay_exact_equality = true;
      }
      processResponseLossReplay = {
        persisted_phase_after_first_response: advanced.value.phase,
        resumed_phase: replayedProcess.value.phase,
        financial_effect_repeated: false,
        exact_financials_preserved: true,
        snapshot_scope: cancellationScope === 'ONE_CANDIDATE' ? 'CANDIDATE' : 'WHOLE_BATCH',
        result_contract_replay_exact_equality: terminalNoMoney ? true : null
      };`,
    'generic process response-loss post-replay snapshot'
  );
  source = replaceExactlyOnce(
    source,
    '  return {\n    normalized,',
    `  return {
    h12_request_id: requestId,
    h12_operation_id: operationId,
    h12_contract_cutover: h12ContractCutover,
    h12_fixture_workbench_convergence: h12FixtureWorkbenchConvergence,
    h12_no_money_result_contract: h12NoMoneyResultContract,
    normalized,`,
    'H12 exact request and operation evidence binding'
  );
  assert.match(source, /FAILED_PAYMENT_RELEASE_CONFIRMED_NOT_PAID/);
  assert.doesNotMatch(source, /FAILED_PAYMENT_CONFIRMED_NO_MONEY_RELEASED_BY_USER/);
  assert.match(source, /WORKER_FIXED_DRAFT_CANCELLATION_LOOKUP/);
  assert.match(source, /WORKER_ALL_MATCHING_PAYMENT_STATUS_REQUEST_START/);
  assert.match(source, /readH12WholeBatchCancellationFinancials\(target, batchId, requestId\)/);
  assert.match(source, /snapshot_scope: cancellationScope === 'ONE_CANDIDATE' \? 'CANDIDATE' : 'WHOLE_BATCH'/);
  return source;
}

function parseRunnerOutput(stdout) {
  const firstBrace = stdout.indexOf('{');
  assert.ok(firstBrace >= 0, `runner JSON missing: ${stdout.slice(-2000)}`);
  return JSON.parse(stdout.slice(firstBrace));
}

function queryOwnedCloneJson(engine, sql) {
  assert.match(engine.targetDatabase, /^(?:h2_cancel_v8|h12_rg5_builder_cancel)_pg(?:17|18)$/);
  const result = docker(engine.container, [
    'env',
    'PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms -c idle_in_transaction_session_timeout=30s -c jit=off',
    'psql', '-U', 'postgres', '-d', engine.targetDatabase,
    '-X', '-v', 'ON_ERROR_STOP=1', '-Atq', '-c', sql
  ]);
  const rows = String(result.output || '').trim().split(/\r?\n/).filter(Boolean);
  assert.ok(rows.length > 0, 'compatibility evidence query returned no row');
  return JSON.parse(rows.at(-1));
}

function readCommunicationCompatibilityEvidence(engine, channel, contractCase) {
  const batchId = engine.batches[channel];
  const routeApplyOwner = contractCase.route === 'PRE_BANK'
    ? 'pay_pre_bank_cancel_apply_work_item'
    : 'pay_no_money_unwind_apply_work_item';
  return queryOwnedCloneJson(engine, `
    WITH request_scope AS (
      SELECT request_row.*
      FROM public.pay_payment_correction_requests AS request_row
      WHERE request_row.pay_batch_id = '${batchId}'::uuid
      ORDER BY request_row.requested_at_utc DESC, request_row.id DESC
      LIMIT 1
    ), operation_scope AS (
      SELECT operation_row.*
      FROM public.banking_pay_operations AS operation_row
      JOIN request_scope AS request_row
        ON operation_row.operation_type = 'PAYMENT_CORRECTION'
       AND operation_row.input_json->>'correction_request_id' = request_row.id::text
    ), work_scope AS (
      SELECT work_item_row.*
      FROM public.pay_payment_correction_work_items AS work_item_row
      JOIN request_scope AS request_row
        ON request_row.id = work_item_row.correction_request_id
    ), selected_candidates AS (
      SELECT membership.pay_batch_candidate_id
      FROM public.pay_payment_correction_request_candidates AS membership
      JOIN request_scope AS request_row
        ON request_row.id = membership.correction_request_id
    ), selected_items AS (
      SELECT item_row.*
      FROM public.pay_batch_items AS item_row
      WHERE item_row.pay_batch_candidate_id IN (
        SELECT selected_candidate.pay_batch_candidate_id
        FROM selected_candidates AS selected_candidate
      )
    ), mail_state AS (
      SELECT fixture_row.fixture_kind,
             fixture_row.expected_status,
             fixture_row.expected_attempt_lease_token,
             fixture_row.immutable_row_md5,
             mail_row.status::text AS actual_status,
             mail_row.attempt_lease_token AS actual_attempt_lease_token,
             CASE WHEN mail_row.id IS NULL THEN NULL::text
               ELSE pg_catalog.md5(pg_catalog.to_jsonb(mail_row)::text)
             END AS actual_row_md5
      FROM private.h12_source_less_cancellation_mail_fixture_v1 AS fixture_row
      LEFT JOIN public.mail_outbox AS mail_row
        ON mail_row.id = fixture_row.mail_outbox_id
    )
    SELECT pg_catalog.jsonb_build_object(
      'request_status', request_row.status,
      'request_contract', pg_catalog.jsonb_build_object(
        'candidate_scope_contract_version', NULLIF(request_row.plan_json->>'candidate_scope_contract_version', '')::integer,
        'candidate_scope_hash_version', NULLIF(request_row.plan_json->>'candidate_scope_hash_version', '')::integer,
        'source_row_count_semantics', request_row.plan_json->>'source_row_count_semantics',
        'communication_cleanup_contract_version', NULLIF(request_row.plan_json->>'communication_cleanup_contract_version', '')::integer
      ),
      'operation_statuses', COALESCE((
        SELECT pg_catalog.jsonb_agg(operation_row.status ORDER BY operation_row.status)
        FROM operation_scope AS operation_row
      ), '[]'::jsonb),
      'operation_phases', COALESCE((
        SELECT pg_catalog.jsonb_agg(operation_row.phase ORDER BY operation_row.phase)
        FROM operation_scope AS operation_row
      ), '[]'::jsonb),
      'work_contracts', COALESCE((
        SELECT pg_catalog.jsonb_agg(contract_row.value ORDER BY contract_row.value::text)
        FROM (
          SELECT DISTINCT pg_catalog.jsonb_build_object(
            'candidate_scope_contract_version', NULLIF(work_item_row.selection_json->>'candidate_scope_contract_version', '')::integer,
            'candidate_scope_hash_version', NULLIF(work_item_row.selection_json->>'candidate_scope_hash_version', '')::integer,
            'source_row_count_semantics', work_item_row.selection_json->>'source_row_count_semantics',
            'communication_cleanup_contract_version', NULLIF(work_item_row.selection_json->>'communication_cleanup_contract_version', '')::integer
          ) AS value
          FROM work_scope AS work_item_row
        ) AS contract_row
      ), '[]'::jsonb),
      'work_status_counts', COALESCE((
        SELECT pg_catalog.jsonb_object_agg(status_count.status, status_count.row_count ORDER BY status_count.status)
        FROM (
          SELECT work_item_row.status, pg_catalog.count(*)::integer AS row_count
          FROM work_scope AS work_item_row
          GROUP BY work_item_row.status
        ) AS status_count
      ), '{}'::jsonb),
      'work_blocker_codes', COALESCE((
        SELECT pg_catalog.jsonb_agg(blocker_code.code ORDER BY blocker_code.code)
        FROM (
          SELECT COALESCE(
            work_item_row.result_json#>>'{blocker,code}',
            work_item_row.result_json->>'code',
            work_item_row.result_json->>'result_code'
          ) AS code
          FROM work_scope AS work_item_row
        ) AS blocker_code
        WHERE blocker_code.code IS NOT NULL
      ), '[]'::jsonb),
      'selected_candidate_count', (SELECT pg_catalog.count(*)::integer FROM selected_candidates),
      'selected_active_item_count', (SELECT pg_catalog.count(*)::integer FROM selected_items WHERE COALESCE(is_voided, false) IS FALSE),
      'selected_voided_item_count', (SELECT pg_catalog.count(*)::integer FROM selected_items WHERE COALESCE(is_voided, false) IS TRUE),
      'selected_voided_ex_vat_pence', (
        SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount_ex_vat), 0) * 100)::bigint
        FROM selected_items WHERE COALESCE(is_voided, false) IS TRUE
      ),
      'correction_item_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_items AS correction_item
        JOIN request_scope AS current_request
          ON current_request.id = correction_item.correction_request_id
      ),
      'mail_fixture_count', (SELECT pg_catalog.count(*)::integer FROM mail_state),
      'mail_changed_or_missing_count', (
        SELECT pg_catalog.count(*)::integer FROM mail_state
        WHERE actual_row_md5 IS DISTINCT FROM immutable_row_md5
      ),
      'mail_status_mismatch_count', (
        SELECT pg_catalog.count(*)::integer FROM mail_state
        WHERE actual_status IS DISTINCT FROM expected_status
      ),
      'mail_lease_mismatch_count', (
        SELECT pg_catalog.count(*)::integer FROM mail_state
        WHERE actual_attempt_lease_token IS DISTINCT FROM expected_attempt_lease_token
      ),
      'provider_attempt_count', (SELECT pg_catalog.count(*)::integer FROM public.banking_pay_operation_provider_attempts),
      'provider_event_count', (SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfer_events),
      'candidate_apply_owner_has_communication_v2', pg_catalog.replace(pg_catalog.pg_get_functiondef(
        ('public.${routeApplyOwner}(uuid,uuid)'::regprocedure)::oid
      ), E'\\n', ' ') ~ 'communication_cleanup_contract_version.{0,240}(=|IS NOT DISTINCT FROM)[[:space:]]*2'
    )::text
    FROM request_scope AS request_row
  `);
}

function compatibilityOutcomeProjection(parsed, channel) {
  const channelResult = parsed.results?.[channel];
  assert.ok(channelResult, JSON.stringify(parsed));
  if (channelResult.normalized) {
    return {
      status: 'PASS',
      normalized: channelResult.normalized
    };
  }
  return {
    status: channelResult.status,
    code: channelResult.code,
    phase: channelResult.phase,
    financial_cancellation_applied: channelResult.financial_cancellation_applied,
    failure_boundary: channelResult.failure_boundary,
    diagnostics: channelResult.diagnostics && {
      request_status: channelResult.diagnostics.request_status,
      operation_status: channelResult.diagnostics.operation_status,
      operation_phase: channelResult.diagnostics.operation_phase,
      batch_status: channelResult.diagnostics.batch_status,
      active_item_count: channelResult.diagnostics.active_item_count,
      voided_item_count: channelResult.diagnostics.voided_item_count,
      total_work_item_count: channelResult.diagnostics.total_work_item_count,
      applied_work_item_count: channelResult.diagnostics.applied_work_item_count,
      blocked_work_item_count: channelResult.diagnostics.blocked_work_item_count
    }
  };
}

function runCompatibilityVariant({ engine, contractCase, transformedRunnerPath, deferCandidateInstall }) {
  const channel = contractCase.channel;
  const env = {
    ...process.env,
    ...ESTABLISHED_CURRENT_BASELINE_FLAGS,
    H2_CANCEL_TARGET: engine.target,
    H2_CANCEL_CHANNEL: channel,
    H2_CANCEL_SCOPE: 'ONE_CANDIDATE',
    H2_CANCEL_PAYMENT_STATE: contractCase.route === 'PRE_BANK' ? 'DRAFT' : 'SCHEDULED_FAILED_NO_MONEY',
    H2_CANCEL_ESTABLISH_CURRENT_SOURCE: 'true',
    H2_CANCEL_DRAIN_WORKBENCH: 'false',
    H2_CANCEL_SIMULATE_RESPONSE_LOSS: 'false',
    H2_CANCEL_KEEP_DATABASE: 'true',
    H2_CANCEL_COMPACT_OUTPUT: 'true',
    H2_CANCEL_INSTRUMENT_REFRESH: 'false',
    H2_CANCEL_INSTRUMENT_PROCESS_ROUTE: 'false',
    H12_SOURCE_LESS_FIXTURE_MODE: 'MAIL_ONLY',
    H12_SOURCE_LESS_PERSISTED_CONTRACT: contractCase.contract,
    H12_SOURCE_LESS_DEFER_CANDIDATE_INSTALL: deferCandidateInstall ? 'true' : 'false'
  };
  const startedAt = performance.now();
  const executed = spawnSync(process.execPath, [transformedRunnerPath], {
    cwd: repoRoot,
    env,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true
  });
  const elapsedMs = Number((performance.now() - startedAt).toFixed(3));
  try {
    assert.equal(executed.status, 0, `${executed.stdout}\n${executed.stderr}`);
    const parsed = parseRunnerOutput(executed.stdout);
    const evidence = readCommunicationCompatibilityEvidence(engine, channel, contractCase);
    return {
      outcome: compatibilityOutcomeProjection(parsed, channel),
      evidence,
      elapsed_ms: elapsedMs
    };
  } finally {
    docker(
      engine.container,
      ['dropdb', '-U', 'postgres', '--if-exists', engine.targetDatabase],
      { allowFailure: true }
    );
  }
}

function runCompactLegacyCompatibility(engineName, engine, baselineRunnerPath, candidateRunnerPath) {
  const contractCase = engineName === 'PG17'
    ? { case_id: 'LEGACY_V1_EXACT_REPLAY', contract: 'LEGACY_V1', route: 'PRE_BANK', channel: 'PAYE' }
    : { case_id: 'LEGACY_V2_COMMUNICATION_V1_EXACT_REPLAY', contract: 'LEGACY_V2_COMMUNICATION_V1', route: 'NO_MONEY', channel: 'UMBRELLA' };
  const baseline = runCompatibilityVariant({
    engine,
    contractCase,
    transformedRunnerPath: baselineRunnerPath,
    deferCandidateInstall: false
  });
  const candidate = runCompatibilityVariant({
    engine,
    contractCase,
    transformedRunnerPath: candidateRunnerPath,
    deferCandidateInstall: true
  });
  assert.deepEqual(candidate.outcome, baseline.outcome, `${contractCase.case_id} outcome drift`);
  const candidateSourceMarker = candidate.evidence.candidate_apply_owner_has_communication_v2;
  const baselineSourceMarker = baseline.evidence.candidate_apply_owner_has_communication_v2;
  delete candidate.evidence.candidate_apply_owner_has_communication_v2;
  delete baseline.evidence.candidate_apply_owner_has_communication_v2;
  assert.deepEqual(candidate.evidence, baseline.evidence, `${contractCase.case_id} durable-state drift`);
  assert.equal(baselineSourceMarker, false);
  assert.equal(candidateSourceMarker, true);
  return {
    ...contractCase,
    status: 'PASS_EXACT_REPLAY',
    baseline_outcome: baseline.outcome,
    candidate_outcome: candidate.outcome,
    durable_state_equal: true,
    candidate_owner_installed_before_apply: true,
    persisted_contract_not_reinterpreted: true,
    baseline_elapsed_ms: baseline.elapsed_ms,
    candidate_elapsed_ms: candidate.elapsed_ms
  };
}

function runPhysicalCase(physicalRun, engine, transformedRunnerPath, snapshotEvidence) {
  assert.ok(snapshotEvidence, `prepared snapshot evidence missing for ${physicalRun.run_id}`);
  const env = {
    ...process.env,
    ...ESTABLISHED_CURRENT_BASELINE_FLAGS,
    H2_CANCEL_TARGET: engine.target,
    H2_CANCEL_CHANNEL: physicalRun.batch_channel,
    H2_CANCEL_SCOPE: physicalRun.scope === 'ONE_CANDIDATE' ? 'ONE_CANDIDATE' : 'WHOLE_BATCH',
    H2_CANCEL_PAYMENT_STATE: physicalRun.route === 'PRE_BANK'
      ? 'DRAFT'
      : 'SCHEDULED_FAILED_NO_MONEY',
    H2_CANCEL_PRODUCTION_SHAPED_SOURCE: 'true',
    H2_CANCEL_ESTABLISH_CURRENT_SOURCE: 'true',
    H2_CANCEL_DRAIN_WORKBENCH: MODE === 'CANDIDATE_GREEN' ? 'true' : 'false',
    H2_CANCEL_SIMULATE_RESPONSE_LOSS: MODE === 'CANDIDATE_GREEN' ? 'true' : 'false',
    H2_CANCEL_KEEP_DATABASE: String(
      process.env.H12_SOURCE_LESS_KEEP_DATABASE || 'false'
    ).trim().toLowerCase() === 'true' ? 'true' : 'false',
    H2_CANCEL_INSTRUMENT_REFRESH: 'false',
    H2_CANCEL_INSTRUMENT_PROCESS_ROUTE: 'false',
    H12_SOURCE_LESS_FIXTURE_MODE: 'FULL',
    H12_SOURCE_LESS_PERSISTED_CONTRACT: '',
    H12_SOURCE_LESS_DEFER_CANDIDATE_INSTALL: 'false',
    H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT: 'true',
    H12_SOURCE_LESS_VERIFY_PREPARED_SNAPSHOT: 'false',
    H12_SOURCE_LESS_PREPARE_SNAPSHOT: 'false',
    H12_SOURCE_LESS_CAPTURE_PRE_CANCELLATION_BOUNDARY: 'false',
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SOURCE: snapshotEvidence.source_fingerprint,
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_SEMANTIC_SHA256: snapshotEvidence.semantic_sha256,
    H12_SOURCE_LESS_EXPECTED_SNAPSHOT_EXACT_SHA256: snapshotEvidence.exact_sha256
  };
  const startedAt = performance.now();
  const result = spawnSync(process.execPath, [transformedRunnerPath], {
    cwd: repoRoot,
    env,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true
  });
  const elapsedMs = Number((performance.now() - startedAt).toFixed(3));
  if (MODE === 'CURRENT_RED') {
    const observed = [result.stdout, result.stderr].filter(Boolean).join('\n');
    const normalizedObserved = observed.replaceAll('\\', '');
    let parsedFailure = null;
    let ambiguityActivation = null;
    if (result.status === 0) {
      const parsed = parseRunnerOutput(result.stdout);
      parsedFailure = parsed.results?.[physicalRun.batch_channel] || null;
      ambiguityActivation = parsed.results?._h12_ambiguity_activation || null;
      const cloneBoundary = parsed.results?._h12_prepared_snapshot_clone || null;
      assert.equal(cloneBoundary?.source_fingerprint, snapshotEvidence.source_fingerprint);
      assert.equal(cloneBoundary?.semantic_sha256, snapshotEvidence.semantic_sha256);
      assert.equal(cloneBoundary?.exact_sha256, snapshotEvidence.exact_sha256);
      assert.ok(parsedFailure && !parsedFailure.normalized, `current owner unexpectedly passed ${physicalRun.run_id}`);
    }
    assert.ok(
      ambiguityActivation?.detector,
      `current-red detector evidence absent for ${physicalRun.run_id}: ${observed.slice(-10000)}`
    );
    assert.equal(ambiguityActivation.detector.ambiguous_minimum, physicalRun.reasons.length);
    assert.equal(ambiguityActivation.detector.ambiguous_maximum, physicalRun.reasons.length);
    assert.equal(ambiguityActivation.detector.automatic_count, 0);
    const scopeSymptom = physicalRun.route === 'NO_MONEY'
      ? {
          boundary: 'CURRENT_PAYMENT_STATUS_RELEASE_ACTION_ADMISSION',
          present: normalizedObserved.includes('"rows":[]')
            && normalizedObserved.includes('"eligible_matching_count":0'),
          token: 'rows=[]/eligible_matching_count=0'
        }
      : physicalRun.scope === 'ONE_CANDIDATE'
      ? {
          boundary: 'CURRENT_PAYMENT_STATUS_ACTION_ADMISSION',
          present: observed.includes('draft_cancel_eligible') && observed.includes('false'),
          token: 'draft_cancel_eligible=false'
        }
      : {
          boundary: 'ALL_MATCHING_SELECTION_PREPARATION',
          present: observed.includes('PAYMENT_CORRECTION_SELECTION_EMPTY')
            && observed.includes('DESCRIPTOR_INVALID'),
          token: 'PAYMENT_CORRECTION_SELECTION_EMPTY/DESCRIPTOR_INVALID'
        };
    assert.ok(
      scopeSymptom.present,
      `current-red cancellation admission veto absent for ${physicalRun.run_id}: ${observed.slice(-5000)}`
    );
    return {
      ...physicalRun,
      status: 'EXPECTED_CURRENT_RED_ADMISSION_VETO',
      process_exit_status: result.status,
      captured_failure_status: parsedFailure?.status || null,
      failure_token: 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS',
      observed_prechange_boundary: scopeSymptom.boundary,
      observed_prechange_symptom: scopeSymptom.token,
      runtime_first_divergence: physicalRun.route === 'NO_MONEY'
        ? 'RELEASE_FAILED_PAYMENT_ACTION_WITHHELD_FOR_CONFIRMED_NO_MONEY_SOURCE_LESS_AMBIGUITY'
        : physicalRun.scope === 'ONE_CANDIDATE'
        ? 'DRAFT_CANCEL_ACTION_WITHHELD_FOR_CONFIRMED_UNPAID_SOURCE_LESS_AMBIGUITY'
        : 'ALL_MATCHING_FILTERS_EVERY_SOURCE_LESS_CANDIDATE_THEN_RETURNS_EMPTY_SELECTION',
      detector_ambiguous_count_per_candidate: physicalRun.reasons.length,
      financial_cancellation_applied: false,
      prepared_snapshot_exact_sha256: snapshotEvidence.exact_sha256,
      prepared_snapshot_verified_before_mutation: true,
      elapsed_ms: elapsedMs
    };
  }
  if (result.status !== 0) {
    throw new Error([
      `physical run ${physicalRun.run_id} failed`,
      result.stdout,
      result.stderr
    ].filter(Boolean).join('\n'));
  }
  const parsed = parseRunnerOutput(result.stdout);
  const cloneBoundary = parsed.results?._h12_prepared_snapshot_clone || null;
  assert.equal(cloneBoundary?.source_fingerprint, snapshotEvidence.source_fingerprint);
  assert.equal(cloneBoundary?.semantic_sha256, snapshotEvidence.semantic_sha256);
  assert.equal(cloneBoundary?.exact_sha256, snapshotEvidence.exact_sha256);
  const channelResult = parsed.results?.[physicalRun.batch_channel];
  assert.ok(channelResult, JSON.stringify(parsed));
  assert.ok(channelResult.normalized, JSON.stringify(channelResult));
  assert.ok(channelResult.h12_source_less, JSON.stringify(channelResult));
  assert.equal(channelResult.normalized.provider_attempt_delta, 0);
  assert.equal(channelResult.normalized.provider_event_delta, 0);
  assert.deepEqual(
    {
      same_request: channelResult.normalized.prepare_response_loss_replay?.same_request,
      same_operation: channelResult.normalized.prepare_response_loss_replay?.same_operation,
      existing_request: channelResult.normalized.prepare_response_loss_replay?.existing_request
    },
    { same_request: true, same_operation: true, existing_request: true }
  );
  assert.equal(channelResult.normalized.process_response_loss_replay?.financial_effect_repeated, false);
  assert.equal(channelResult.normalized.process_response_loss_replay?.exact_financials_preserved, true);
  assert.equal(
    channelResult.normalized.process_response_loss_replay?.snapshot_scope,
    physicalRun.scope === 'ONE_CANDIDATE' ? 'CANDIDATE' : 'WHOLE_BATCH'
  );
  const noMoneyResultContract = channelResult.h12_source_less.no_money_result_contract ?? null;
  if (physicalRun.route === 'NO_MONEY') {
    assert.ok(noMoneyResultContract, JSON.stringify(channelResult.h12_source_less));
    assert.equal(noMoneyResultContract.expected_top_level_result_key_count, 82);
    assert.equal(noMoneyResultContract.expected_full_persisted_key_count, 85);
    assert.deepEqual(
      noMoneyResultContract.expected_persisted_wrapper_keys,
      ['candidate_scope_hash', 'created_by', 'selection_ordinal']
    );
    assert.equal(noMoneyResultContract.returned_top_level_result_keys_complete, true);
    assert.equal(noMoneyResultContract.full_persisted_result_keys_complete, true);
    assert.equal(noMoneyResultContract.persisted_top_level_result_keys_complete, true);
    assert.equal(noMoneyResultContract.explicit_null_key_presence_verified, true);
    assert.equal(noMoneyResultContract.returned_persisted_contract_jsonb_equal, true);
    assert.equal(noMoneyResultContract.returned_persisted_contract_canonical_text_equal, true);
    assert.equal(noMoneyResultContract.returned_persisted_contract_sha256_equal, true);
    assert.equal(noMoneyResultContract.process_response_loss_replay_exact_equality, true);
    assert.equal(noMoneyResultContract.exact_request_replay_exact_equality, true);
    assert.equal(
      channelResult.normalized.process_response_loss_replay?.result_contract_replay_exact_equality,
      true
    );
  } else {
    assert.equal(noMoneyResultContract, null);
  }
  assert.ok(channelResult.maximum_call_ms < 15000, JSON.stringify(channelResult.phase_calls));
  for (const call of channelResult.phase_calls) {
    if (['pay_payment_correction_request_start', 'pay_payment_correction_reauth_bind_v1', 'pay_payment_correction_process_chunk', 'pay_batch_cancel'].includes(call.rpc)) {
      assert.ok(call.elapsed_ms < 6000, JSON.stringify(call));
    }
  }
  const workbenchDrain = channelResult.workbench_drain || null;
  const workbenchSteps = Array.isArray(workbenchDrain?.safe_steps)
    ? workbenchDrain.safe_steps
    : [];
  const sourceBuildSteps = workbenchSteps.filter((step) => step.route === 'SOURCE_BUILD');
  const normalSteps = workbenchSteps.filter((step) => step.route === 'NORMAL');
  const scanSteps = workbenchSteps.filter((step) => step.result_code === 'CLAIM_SCAN_CURSOR_WRAPPED');
  const workbenchCallDurations = workbenchSteps.flatMap((step) => [
    step.elapsed_ms,
    step.claim_elapsed_ms,
    step.execute_elapsed_ms
  ]).filter((value) => Number.isFinite(value));
  return {
    ...physicalRun,
    status: 'CANDIDATE_GREEN',
    request_status: channelResult.h12_source_less.request_status,
    operation_status: channelResult.h12_source_less.operation_status,
    communication_cleanup_contract_version: channelResult.h12_source_less.communication_cleanup_contract_version,
    selected_fixture_candidate_count: channelResult.h12_source_less.selected_fixture_candidate_count,
    selected_safe_count: channelResult.h12_source_less.selected_safe_count,
    selected_ambiguous_count: channelResult.h12_source_less.selected_ambiguous_count,
    safe_carry_forward_count: channelResult.h12_source_less.safe_carry_forward_count,
    ambiguous_carry_forward_count: channelResult.h12_source_less.ambiguous_carry_forward_count,
    ambiguous_alert_evidence_count: channelResult.h12_source_less.ambiguous_alert_evidence_count,
    mail_fixture_count: channelResult.h12_source_less.mail_fixture_count,
    mail_changed_or_missing_count: channelResult.h12_source_less.mail_changed_or_missing_count,
    completed_request_replay_same_request: channelResult.h12_source_less.completed_request_replay_same_request,
    completed_request_replay_same_operation: channelResult.h12_source_less.completed_request_replay_same_operation,
    completed_request_replay_route: channelResult.h12_source_less.completed_request_replay_route,
    no_money_result_contract: noMoneyResultContract,
    prepare_response_loss_replay: channelResult.normalized.prepare_response_loss_replay,
    process_response_loss_replay: channelResult.normalized.process_response_loss_replay,
    workbench_drain: workbenchDrain ? {
      iteration_count: workbenchDrain.iteration_count,
      elapsed_ms: workbenchDrain.elapsed_ms,
      terminal_current_count: workbenchDrain.currentness?.terminal_current_count,
      candidate_count: workbenchDrain.currentness?.candidate_count,
      normal_step_count: normalSteps.length,
      normal_processed_count: normalSteps.reduce((sum, step) => sum + Number(step.processed || 0), 0),
      source_build_step_count: sourceBuildSteps.length,
      source_build_stage_counts: Object.fromEntries(
        [...new Set(sourceBuildSteps.map((step) => step.private_stage || 'UNKNOWN'))]
          .sort()
          .map((stage) => [stage, sourceBuildSteps.filter((step) => (step.private_stage || 'UNKNOWN') === stage).length])
      ),
      scan_progress_step_count: scanSteps.length,
      maximum_individual_call_ms: workbenchCallDurations.length
        ? Math.max(...workbenchCallDurations)
        : null
    } : null,
    maximum_call_ms: channelResult.maximum_call_ms,
    prepared_snapshot_exact_sha256: snapshotEvidence.exact_sha256,
    prepared_snapshot_verified_before_mutation: true,
    elapsed_ms: elapsedMs
  };
}

if (MAIL_MULTICANDIDATE_SCALE_ONLY) {
  assert.equal(MODE, 'CANDIDATE_GREEN', '--mail-multicandidate-scale requires CANDIDATE_GREEN mode');
  const engineFilter = selectedFilter(process.env.H12_SOURCE_LESS_ENGINE);
  const selected = Object.entries(engines).filter(([engineName]) => (
    !engineFilter.size || engineFilter.has(engineName)
  ));
  assert.ok(selected.length > 0, 'engine filter selected no scale target');
  const scaleResults = selected.map(([engineName, engine]) => (
    runMailMulticandidateScale(engineName, engine)
  ));
  console.log(JSON.stringify({
    contract: 'H12_PAYMENT_CANCELLATION_NOTICE_5000_CANDIDATE_SCALE_V1',
    status: 'PASS',
    runtime_authority: 'LOCAL_DISPOSABLE_PG17_PG18_ONLY',
    results: scaleResults,
    policy_or_economic_change: false,
    timeout_relaxation: false
  }, null, 2));
  process.exit(0);
}

const fixture = validateFixture(JSON.parse(fs.readFileSync(fixturePath, 'utf8')));
const logicalCases = expandLogicalCases(fixture);
const allPhysicalRuns = buildPhysicalRuns(logicalCases);
const physicalRuns = filterPhysicalRuns(allPhysicalRuns);
assert.ok(physicalRuns.length > 0, 'filters selected no physical runs');
const candidatePaths = candidateSourcePaths();
const candidateSourceSha256Before = candidatePaths.map((absolute) => ({
  path: path.relative(repoRoot, absolute).replaceAll('\\', '/'),
  bytes: fs.statSync(absolute).size,
  sha256: sha256(fs.readFileSync(absolute))
}));

if (CHECK_RESULT_PATH_ONLY) {
  const checkedEngineNames = [...new Set(physicalRuns.map((row) => row.engine))];
  const checkedResultPath = resolveSafeResultPath(checkedEngineNames);
  console.log(JSON.stringify({
    contract: 'H12_RESULT_PATH_CONTAINMENT_CHECK_V1',
    ok: true,
    selected_engines: checkedEngineNames,
    result_path: checkedResultPath,
    direct_child_only: true,
    reparse_points_rejected: true,
    runtime_executed: false
  }, null, 2));
  process.exit(0);
}

if (CHECK_TRANSFORM_ONLY) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'h12-source-less-transform-check-'));
  try {
    const checked = [];
    for (const [engineName, engine] of Object.entries(engines)) {
      const transformedRunnerPath = path.join(tempDir, `runner-${engineName.toLowerCase()}.mjs`);
      fs.writeFileSync(transformedRunnerPath, buildTransformedRunner(engine, candidatePaths));
      run(process.execPath, ['--check', transformedRunnerPath]);
      checked.push(engineName);
    }
    console.log(JSON.stringify({
      contract: 'H12_SOURCE_LESS_CANCELLATION_TRANSFORM_CHECK_V1',
      ok: true,
      checked_engines: checked,
      candidate_source_count: candidatePaths.length,
      runtime_executed: false
    }, null, 2));
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
  process.exit(0);
}

if (DESCRIBE_ONLY) {
  console.log(JSON.stringify({
    contract: fixture.artifact,
    mode: MODE,
    logical_case_count: logicalCases.length,
    physical_run_count: allPhysicalRuns.length,
    selected_physical_run_count: physicalRuns.length,
    candidate_source_count: candidatePaths.length,
    logical_cases: logicalCases,
    physical_runs: physicalRuns,
    runtime_executed: false
  }, null, 2));
  process.exit(0);
}

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'h12-source-less-cancel-'));
const runtimeResults = [];
const selectedEngineNames = [...new Set(physicalRuns.map((row) => row.engine))];
const resultPath = resolveSafeResultPath(selectedEngineNames);
const snapshotEvidenceByEngine = {};
const baselineLifecycleStarted = new Set();
const snapshotLifecycleStarted = new Set();
let outerExecutionCompleted = false;
try {
  for (const engineName of selectedEngineNames) {
    const engine = engines[engineName];
    assert.ok(engine, engineName);
    const transformedRunnerPath = path.join(tempDir, `runner-${engineName.toLowerCase()}.mjs`);
    fs.writeFileSync(transformedRunnerPath, buildTransformedRunner(engine, candidatePaths));
    let snapshotEvidence;
    if (REUSE_VERIFIED_PREPARED_SNAPSHOT) {
      snapshotEvidence = verifyExistingH12Snapshot(engineName, engine, transformedRunnerPath);
    } else {
      baselineLifecycleStarted.add(engineName);
      prepareTemplateDatabase(engine);
      snapshotLifecycleStarted.add(engineName);
      snapshotEvidence = prepareAndVerifyH12Snapshot(engineName, engine, transformedRunnerPath);
    }
    snapshotEvidenceByEngine[engineName] = snapshotEvidence;
    if (!SNAPSHOT_EQUIVALENCE_ONLY) {
      for (const physicalRun of physicalRuns.filter((row) => row.engine === engineName)) {
        runtimeResults.push(runPhysicalCase(physicalRun, engine, transformedRunnerPath, snapshotEvidence));
      }
    }
  }
  outerExecutionCompleted = true;
} finally {
  for (const engineName of selectedEngineNames) {
    if (!outerExecutionCompleted
        && snapshotLifecycleStarted.has(engineName)
        && !KEEP_FAILED_TARGET_DATABASE) {
      dropTaskOwnedTargetDatabase(engines[engineName]);
    }
    if (snapshotLifecycleStarted.has(engineName)) dropPreparedTemplateDatabase(engines[engineName]);
    if (baselineLifecycleStarted.has(engineName)) dropTemplateDatabase(engines[engineName]);
  }
  if (KEEP_TRANSFORMED_RUNNER) {
    console.error(`H12 transformed runner retained at ${tempDir}`);
  } else {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

if (SNAPSHOT_EQUIVALENCE_ONLY) {
  const snapshotOutput = {
    contract: 'H12_LEGACY_FRESH_PREPARED_CLONE_EQUIVALENCE_V1',
    status: 'PASS',
    mode: MODE,
    base_runner_sha256: BASE_RUNNER_SHA256,
    selected_engines: selectedEngineNames,
    evidence: snapshotEvidenceByEngine,
    route_fixture_scope_mutation_executed: false,
    runtime_matrix_executed: false,
    policy_or_economic_change: false,
    test_database_containment: 'TASK_OWNED_PREPARED_TEMPLATES_AND_CLONES_REMOVED'
  };
  if (String(process.env.H12_SOURCE_LESS_WRITE_RESULT || '').trim().toLowerCase() === 'true') {
    assertSafeResultDestination(resultPath);
    fs.mkdirSync(resultDirectory, { recursive: true });
    fs.writeFileSync(resultPath, `${JSON.stringify(snapshotOutput, null, 2)}\n`);
  }
  console.log(JSON.stringify(snapshotOutput, null, 2));
  process.exit(0);
}

const coveredLogicalCaseIds = runtimeResults.flatMap((row) => row.logical_case_ids);
assert.equal(
  coveredLogicalCaseIds.length,
  physicalRuns.reduce((count, physicalRun) => count + physicalRun.logical_case_ids.length, 0)
);
assert.equal(new Set(coveredLogicalCaseIds).size, coveredLogicalCaseIds.length);

const candidateSourceSha256After = candidatePaths.map((absolute) => ({
  path: path.relative(repoRoot, absolute).replaceAll('\\', '/'),
  bytes: fs.statSync(absolute).size,
  sha256: sha256(fs.readFileSync(absolute))
}));
assert.deepEqual(
  candidateSourceSha256After,
  candidateSourceSha256Before,
  'candidate SQL changed while the runtime matrix was executing'
);

const output = {
  contract: 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_RESULTS_V1',
  status: MODE === 'CURRENT_RED' ? 'EXPECTED_PRECHANGE_DIVERGENCE' : 'PASS',
  source_head: fixture.source_head,
  mode: MODE,
  base_runner_sha256: BASE_RUNNER_SHA256,
  candidate_source_paths: candidatePaths.map((absolute) => path.relative(repoRoot, absolute).replaceAll('\\', '/')),
  candidate_source_sha256_before: candidateSourceSha256Before,
  candidate_source_sha256_after: candidateSourceSha256After,
  candidate_source_hashes_unchanged: true,
  selected_logical_case_count: coveredLogicalCaseIds.length,
  selected_physical_run_count: runtimeResults.length,
  prepared_snapshot_equivalence: snapshotEvidenceByEngine,
  results: runtimeResults,
  policy_x_preserved: true,
  timeout_relaxation: false,
  exact_function_budgets: {
    prepare_expand_apply_statement_timeout_ms: fixture.candidate_green_contract.prepare_expand_apply_statement_timeout_ms,
    prepare_expand_apply_lock_timeout_ms: fixture.candidate_green_contract.prepare_expand_apply_lock_timeout_ms,
    integrity_checker_statement_timeout_ms: fixture.candidate_green_contract.integrity_checker_statement_timeout_ms,
    status_page_statement_timeout_ms: fixture.status_page_admission_reconciliation.current_owner.statement_timeout_ms
  },
  outer_harness_budgets: {
    statement_timeout_ms: fixture.candidate_green_contract.outer_harness_statement_timeout_ms,
    lock_timeout_ms: fixture.candidate_green_contract.outer_harness_lock_timeout_ms,
    idle_in_transaction_session_timeout_ms: fixture.candidate_green_contract.outer_harness_idle_in_transaction_timeout_ms
  },
  provider_settlement_remittance_actions: 0,
  test_database_containment: 'TASK_OWNED_DISPOSABLE_CLONES_REMOVED'
};

if (String(process.env.H12_SOURCE_LESS_WRITE_RESULT || '').trim().toLowerCase() === 'true') {
  assertSafeResultDestination(resultPath);
  fs.mkdirSync(resultDirectory, { recursive: true });
  fs.writeFileSync(resultPath, `${JSON.stringify(output, null, 2)}\n`);
}
console.log(JSON.stringify(output, null, 2));
