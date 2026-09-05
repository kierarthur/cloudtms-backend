import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const historicalPath = resolve(root, 'supabase/repeatable/04082026_1154_pay_batches_claim_due_scheduled.sql');
const replacementPath = resolve(root, 'supabase/repeatable/04092026_2330_banking_pay_due_schedule_local_prepare_evidence_v1.sql');
const runtimePath = resolve(root, 'tests/04092026_2340_banking_pay_scheduled_due_local_prepare_runtime.sql');
const historicalSql = readFileSync(historicalPath, 'utf8');
const replacementSql = readFileSync(replacementPath, 'utf8');
const runtimeSql = readFileSync(runtimePath, 'utf8');
const sha256 = value => createHash('sha256').update(value).digest('hex');

const argv = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index];
  const value = process.argv[index + 1];
  if (!key?.startsWith('--') || !value) throw new Error(`Invalid argument at ${index}`);
  argv.set(key.slice(2), value);
}

const engines = [
  {
    name: 'pg17',
    container: argv.get('pg17-container') || 'h12-v8-restart-pg17',
    database: argv.get('pg17-database') || 'banking_modal_v2_test',
    batchId: argv.get('pg17-batch') || 'fda06a34-168f-468f-98b8-e9f3fe9d029d'
  },
  {
    name: 'pg18',
    container: argv.get('pg18-container') || 'h12-v8-restart-pg18',
    database: argv.get('pg18-database') || 'banking_modal_v2_test',
    batchId: argv.get('pg18-batch') || 'd8a00679-fd2f-4def-8a1f-f0350ca597a4'
  }
];

const safeName = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
const safeDatabase = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
for (const engine of engines) {
  if (!safeName.test(engine.container)) throw new Error(`Unsafe container: ${engine.container}`);
  if (!safeDatabase.test(engine.database)) throw new Error(`Unsafe database: ${engine.database}`);
  if (!uuid.test(engine.batchId)) throw new Error(`Unsafe batch ID: ${engine.batchId}`);
}

function psql(engine, input, extraArgs = []) {
  const started = performance.now();
  const result = spawnSync('docker', [
    'exec', '-i', '-e', 'PGOPTIONS=-c jit=off', engine.container,
    'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', engine.database,
    ...extraArgs
  ], { input, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  const elapsedMs = Math.round((performance.now() - started) * 100) / 100;
  if (result.status !== 0) {
    throw new Error(`${engine.name} psql failed (${result.status}):\n${result.stderr || result.stdout}`);
  }
  return { stdout: String(result.stdout || ''), stderr: String(result.stderr || ''), elapsedMs };
}

function install(engine, source) {
  return psql(engine, source);
}

function runFixture(engine, evidenceMode = 'LOCAL_ONLY') {
  const result = psql(engine, runtimeSql, [
    '-v', `h2_batch_id=${engine.batchId}`,
    '-v', `h2_evidence_mode=${evidenceMode}`
  ]);
  const marker = '__H2_EXECUTE_PREP__';
  const line = result.stdout.split(/\r?\n/).find(value => value.startsWith(marker));
  if (!line) throw new Error(`${engine.name} result marker missing:\n${result.stdout}`);
  return { payload: JSON.parse(line.slice(marker.length)), elapsedMs: result.elapsedMs };
}

function assertCommon(engine, payload) {
  const fail = message => { throw new Error(`${engine.name}: ${message}`); };
  if (payload.prepare?.ok !== true || payload.prepare?.failed_count !== 0 || payload.prepare?.has_more !== false) fail(`transfer preparation failed: ${JSON.stringify(payload.prepare)}`);
  if (payload.batch_prepare?.ok !== true || payload.batch_prepare?.ready !== true || payload.batch_prepare?.blocker_count !== 0) fail(`batch preparation failed: ${JSON.stringify(payload.batch_prepare)}`);
  if (payload.auth?.ok !== true || payload.auth?.state !== 'AUTHORISED' || payload.auth?.next_required_phase !== 'WAIT_FOR_SCHEDULE') fail(`authorisation mismatch: ${JSON.stringify(payload.auth)}`);
  if (payload.schedule?.ok !== true || payload.schedule?.batch_status !== 'SCHEDULED') fail(`schedule mismatch: ${JSON.stringify(payload.schedule)}`);
  if (payload.provider_attempt_count !== 0 || payload.provider_event_count !== 0) fail('fixture crossed the provider boundary');
  if (!Array.isArray(payload.canonical_transfer_classifier) || payload.canonical_transfer_classifier.length === 0) fail('canonical transfer classification missing');
  for (const row of payload.canonical_transfer_classifier) {
    if (row.evidence_classification !== 'SCHEDULED_LOCAL_NOT_SENT'
      || row.has_local_prepare_identity !== true
      || row.has_provider_submission_evidence !== false
      || row.has_provider_event_evidence !== false
      || row.has_provider_attempt_without_external_id !== false
      || row.has_operation_submit_attempt !== false
      || row.has_ambiguous_external_evidence !== false
      || row.is_unattempted_submit_eligible !== true
      || row.has_provider_submit_blocker !== false) {
      fail(`canonical local-unsent classification mismatch: ${JSON.stringify(row)}`);
    }
  }
}

function assertPreChange(engine, payload) {
  assertCommon(engine, payload);
  if (payload.due_claim?.code !== 'NO_DUE_BATCH'
    || payload.due_claim?.claimed_count !== 0
    || payload.due_claim?.skipped_count !== 1
    || payload.due_claim?.operations?.[0]?.code !== 'PROVIDER_EVIDENCE_PRESENT') {
    throw new Error(`${engine.name}: pre-change divergence was not reproduced exactly: ${JSON.stringify(payload.due_claim)}`);
  }
}

function assertCorrected(engine, payload) {
  assertCommon(engine, payload);
  const operation = payload.due_claim?.operations?.[0] || {};
  if (payload.due_claim?.code !== 'DUE_OPERATION_STARTED'
    || payload.due_claim?.claimed_count !== 1
    || payload.due_claim?.skipped_count !== 0
    || operation.code !== 'DUE_OPERATION_STARTED'
    || operation.operation_created !== true
    || !uuid.test(String(operation.operation_id || ''))) {
    throw new Error(`${engine.name}: corrected due claim mismatch: ${JSON.stringify(payload.due_claim)}`);
  }
  if (payload.due_claim_replay?.code !== 'NO_DUE_BATCH' || payload.due_claim_replay?.claimed_count !== 0) {
    throw new Error(`${engine.name}: replay was not idempotent: ${JSON.stringify(payload.due_claim_replay)}`);
  }
}

function assertUnsafeEvidenceBlocked(engine, evidenceMode, payload) {
  if (payload.prepare?.ok !== true || payload.batch_prepare?.ready !== true
    || payload.auth?.state !== 'AUTHORISED' || payload.schedule?.batch_status !== 'SCHEDULED') {
    throw new Error(`${engine.name}/${evidenceMode}: prerequisite route failed`);
  }
  if (payload.due_claim?.claimed_count !== 0 || payload.due_claim_replay?.claimed_count !== 0) {
    throw new Error(`${engine.name}/${evidenceMode}: unsafe evidence was claimed: ${JSON.stringify(payload.due_claim)}`);
  }
  const rows = Array.isArray(payload.canonical_transfer_classifier) ? payload.canonical_transfer_classifier : [];
  if (!rows.some(row => row.has_provider_submission_evidence === true
    || row.has_provider_event_evidence === true
    || row.has_provider_attempt_without_external_id === true
    || row.has_operation_submit_attempt === true
    || row.has_ambiguous_external_evidence === true
    || row.has_provider_submit_blocker === true
    || row.is_unattempted_submit_eligible !== true)) {
    throw new Error(`${engine.name}/${evidenceMode}: canonical unsafe classification was not observed`);
  }
}

function semanticSummary(payload) {
  return {
    prepare: {
      ok: payload.prepare.ok,
      failed_count: payload.prepare.failed_count,
      has_more: payload.prepare.has_more,
      next_required_phase: payload.prepare.next_required_phase,
      item_transfer_linked_count: payload.prepare.item_transfer_linked_count
    },
    batch_prepare: {
      ok: payload.batch_prepare.ok,
      ready: payload.batch_prepare.ready,
      blocker_count: payload.batch_prepare.blocker_count,
      next_required_phase: payload.batch_prepare.next_required_phase
    },
    auth: {
      ok: payload.auth.ok,
      state: payload.auth.state,
      next_required_phase: payload.auth.next_required_phase
    },
    schedule: {
      ok: payload.schedule.ok,
      batch_status: payload.schedule.batch_status,
      schedule_kind: payload.schedule.schedule_kind
    },
    due_claim: {
      code: payload.due_claim.code,
      claimed_count: payload.due_claim.claimed_count,
      skipped_count: payload.due_claim.skipped_count,
      operation_code: payload.due_claim.operations?.[0]?.code,
      operation_created: payload.due_claim.operations?.[0]?.operation_created
    },
    replay: {
      code: payload.due_claim_replay.code,
      claimed_count: payload.due_claim_replay.claimed_count
    },
    transfer_count: payload.transfer_count,
    provider_attempt_count: payload.provider_attempt_count,
    provider_event_count: payload.provider_event_count,
    classifier: payload.canonical_transfer_classifier.map(row => ({
      evidence_classification: row.evidence_classification,
      has_local_prepare_identity: row.has_local_prepare_identity,
      has_provider_submission_evidence: row.has_provider_submission_evidence,
      has_provider_event_evidence: row.has_provider_event_evidence,
      has_provider_attempt_without_external_id: row.has_provider_attempt_without_external_id,
      has_operation_submit_attempt: row.has_operation_submit_attempt,
      has_ambiguous_external_evidence: row.has_ambiguous_external_evidence,
      is_unattempted_submit_eligible: row.is_unattempted_submit_eligible,
      has_provider_submit_blocker: row.has_provider_submit_blocker
    }))
  };
}

function readDefinition(engine) {
  const query = String.raw`select pg_get_functiondef('public.pay_batches_claim_due_scheduled(integer,timestamp with time zone)'::regprocedure);`;
  const result = psql(engine, query, ['-A', '-t', '-q']);
  return { sha256: sha256(result.stdout.replace(/\r\n/g, '\n').trimEnd() + '\n'), bytes: Buffer.byteLength(result.stdout.replace(/\r\n/g, '\n').trimEnd() + '\n') };
}

const results = {};
try {
  for (const engine of engines) {
    install(engine, historicalSql);
    const before = runFixture(engine);
    assertPreChange(engine, before.payload);

    install(engine, replacementSql);
    const after = runFixture(engine);
    assertCorrected(engine, after.payload);

    const unsafeEvidence = {};
    for (const evidenceMode of [
      'RAIL_TRANSACTION',
      'REQUEST_SENT_NO_EXTERNAL',
      'PROVIDER_RESPONSE',
      'OPERATION_ATTEMPT',
      'AMBIGUOUS'
    ]) {
      const negative = runFixture(engine, evidenceMode);
      assertUnsafeEvidenceBlocked(engine, evidenceMode, negative.payload);
      unsafeEvidence[evidenceMode] = {
        due_claim_code: negative.payload.due_claim?.code,
        due_claimed_count: negative.payload.due_claim?.claimed_count,
        due_skipped_count: negative.payload.due_claim?.skipped_count,
        classifier: semanticSummary(negative.payload).classifier
      };
    }

    results[engine.name] = {
      pre_change: semanticSummary(before.payload),
      corrected: semanticSummary(after.payload),
      unsafe_evidence_fail_closed: unsafeEvidence,
      timings_ms: { pre_change_fixture: before.elapsedMs, corrected_fixture: after.elapsedMs },
      installed_definition: readDefinition(engine)
    };
  }
} finally {
  for (const engine of engines) install(engine, replacementSql);
}

const pg17Fingerprint = sha256(JSON.stringify(results.pg17.corrected));
const pg18Fingerprint = sha256(JSON.stringify(results.pg18.corrected));
if (pg17Fingerprint !== pg18Fingerprint) {
  throw new Error(`PG17/PG18 semantic result differs: ${pg17Fingerprint} !== ${pg18Fingerprint}`);
}

console.log(JSON.stringify({
  ok: true,
  contract: 'BANKING_PAY_SCHEDULED_DUE_LOCAL_PREPARE_EVIDENCE_V1',
  classification: 'SCHEDULED_EXECUTION_ORCHESTRATION_DEFECT_POLICY_DELTA_ZERO',
  historical_owner_sha256: sha256(historicalSql),
  replacement_owner_sha256: sha256(replacementSql),
  runtime_fixture_sha256: sha256(runtimeSql),
  semantic_result_sha256: pg17Fingerprint,
  policy_or_economic_change_count: 0,
  provider_payment_settlement_remittance_action_count: 0,
  transaction_rolled_back: true,
  statement_timeout_ms: 15000,
  lock_timeout_ms: 1500,
  owner_statement_timeout_ms: 6000,
  owner_lock_timeout_ms: 1000,
  engines: results
}, null, 2));
