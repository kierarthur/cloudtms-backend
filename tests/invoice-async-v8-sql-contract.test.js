import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = relativePath =>
  readFileSync(new URL(relativePath, repoRoot), 'utf8');

const v8RepeatableDirectory = new URL(
  'supabase/repeatable/27072026_1042_invoice_async_v8/',
  repoRoot,
);

const v8RuntimeFiles = [
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_start_batch.sql',
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_work_claim_batch.sql',
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_advance_batch.sql',
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_control_batch.sql',
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_get.sql',
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_advance_batch.sql',
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_operation_rollup_batch.sql',
  'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_issue_advance_batch.sql',
];

test('invoice V8 SQL files use the required timestamped naming convention', () => {
  const migrationFiles = readdirSync(
    new URL('supabase/migrations/', repoRoot),
  ).filter(name =>
    name.startsWith('27072026_1042_')
    || name.startsWith('27072026_1250_')
  );
  const repeatableFiles = readdirSync(v8RepeatableDirectory);

  assert.equal(migrationFiles.length, 5);
  for (const name of [...migrationFiles, ...repeatableFiles]) {
    assert.match(name, /^\d{8}_\d{4}_[a-z0-9_]+\.sql$/);
  }
});

test('V8 runtime authorities do not delegate to preserved legacy copies', () => {
  const newRuntimeFiles = readdirSync(v8RepeatableDirectory)
    .filter(name => !name.endsWith('_19_invoice_async_contract_get_v2.sql'))
    .map(name => read(`supabase/repeatable/27072026_1042_invoice_async_v8/${name}`));
  const runtimeSql = [
    ...newRuntimeFiles,
    ...v8RuntimeFiles.map(read),
  ].join('\n');

  assert.doesNotMatch(runtimeSql, /_legacy_20260726/i);
  assert.doesNotMatch(
    read('supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_09_private_invoice_batch_generate_candidate_rows_v2.sql'),
    /public\.invoice_batch_generate_candidates\s*\(/i,
  );
  assert.doesNotMatch(
    read('supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_10_private_invoice_batch_issue_candidate_rows_v2.sql'),
    /public\.invoice_batch_issue_candidates\s*\(/i,
  );
});

test('selection V2 enforces exact selectors and bounded explicit keys', () => {
  const rules = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_08_private_invoice_batch_selection_rules_v2.sql',
  );
  const generate = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_09_private_invoice_batch_generate_candidate_rows_v2.sql',
  );
  const issue = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_10_private_invoice_batch_issue_candidate_rows_v2.sql',
  );

  for (const selector of [
    'ROW',
    'WEEK',
    'CLIENT',
    'CANDIDATE',
    'STATUS',
    'WEEK_CLIENT',
    'WEEK_CLIENT_CANDIDATE',
    'STATUS_WEEK',
    'STATUS_WEEK_CLIENT',
  ]) {
    assert.match(rules, new RegExp(`'${selector}'`));
  }
  assert.doesNotMatch(rules, /when 'GROUP_KEY'/);
  assert.match(rules, /jsonb_array_length\(v_rules\) > 10000/);
  assert.match(rules, /BATCH_SELECTION_SELECTOR_INVALID/);
  for (const candidateSql of [generate, issue]) {
    assert.match(candidateSql, /v_mode='EXPLICIT_KEYS'/);
    assert.match(candidateSql, /jsonb_array_length\(v_selection_keys\) > 100/);
    assert.match(candidateSql, /BATCH_SOURCE_CHANGED/);
    assert.match(candidateSql, /facet_client_rows as materialized/i);
    assert.match(candidateSql, /facet_blocker_rows as materialized/i);
    assert.match(candidateSql, /next_cursor_values/i);
    assert.match(
      candidateSql,
      /jsonb_typeof\(v_input_snapshot\) = 'null'/,
    );
    assert.doesNotMatch(candidateSql, /25001/);
  }
});

test('candidate snapshots are Vault-backed, signed, and verified in DB', () => {
  const migration = read(
    'supabase/migrations/27072026_1250_invoice_async_v8_contract_corrections.sql',
  );
  const snapshotGet = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_03_private_invoice_candidate_snapshot_get_v2.sql',
  );
  const snapshotVerify = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1250_03a_private_invoice_candidate_snapshot_verify_v2.sql',
  );
  const bump = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_04_private_invoice_candidate_snapshot_bump_v2.sql',
  );

  assert.match(migration, /vault\.create_secret/i);
  assert.doesNotMatch(migration, /secret_bytes/i);
  assert.match(snapshotGet, /extensions\.hmac/i);
  assert.match(snapshotGet, /interval '30 minutes'/i);
  assert.match(snapshotVerify, /BATCH_SNAPSHOT_SIGNATURE_INVALID/);
  assert.match(snapshotVerify, /BATCH_SNAPSHOT_CHANGED/);
  assert.match(snapshotVerify, /pg_advisory_xact_lock_shared/i);
  assert.match(bump, /pg_advisory_xact_lock/i);
});

test('canonical hashes and candidate trigger projections are locked artifacts', () => {
  const vectors = JSON.parse(read(
    'supabase/contracts/27072026_1250_invoice_async_v8_canonical_hash_vectors.json',
  ));
  const triggerManifest = JSON.parse(read(
    'supabase/contracts/27072026_1250_invoice_async_v8_candidate_trigger_manifest.json',
  ));
  const functionHashes = JSON.parse(read(
    'supabase/contracts/27072026_1250_invoice_async_v8_function_hashes.json',
  ));

  assert.equal(vectors.contract_version, 'INVOICE_BATCH_CANONICAL_HASH_V2');
  assert.equal(vectors.vectors.length, 5);
  for (const vector of vectors.vectors) {
    assert.match(vector.sha256, /^[0-9a-f]{64}$/);
  }

  assert.equal(
    triggerManifest.contract_version,
    'INVOICE_ASYNC_TRIGGER_MANIFEST_V2',
  );
  assert.equal(triggerManifest.table_count, 18);
  assert.equal(triggerManifest.candidate_trigger_count, 54);
  assert.equal(triggerManifest.result_trigger_count, 3);
  assert.equal(triggerManifest.tables.length, 18);
  for (const table of triggerManifest.tables) {
    assert.ok(table.field_count > 0);
    assert.match(table.projection_sha256, /^[0-9a-f]{64}$/);
  }

  assert.equal(
    functionHashes.contract_version,
    'INVOICE_ASYNC_DB_V2_FUNCTION_HASH_MANIFEST',
  );
  assert.equal(functionHashes.functions.length, 32);
  assert.match(functionHashes.aggregate_sha256, /^[0-9a-f]{64}$/);
  for (const fn of functionHashes.functions) {
    assert.match(fn.definition_sha256, /^[0-9a-f]{64}$/);
  }
});

test('strict query validation and post-helper trigger installation are present', () => {
  const queryValidation = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1250_08a_private_invoice_batch_query_validate_v2.sql',
  );
  const triggerInstaller = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1250_20_private_invoice_candidate_triggers_install_v2.sql',
  );
  const historicalTriggerMigration = read(
    'supabase/migrations/27072026_1042_04_invoice_async_v8_candidate_revision_triggers.sql',
  );

  assert.match(queryValidation, /INVOICE_BATCH_QUERY_UNKNOWN_FIELD/);
  assert.match(queryValidation, /INVOICE_BATCH_FILTER_UNKNOWN_FIELD/);
  assert.match(queryValidation, /BATCH_FACET_REQUEST_INVALID/);
  assert.match(
    queryValidation,
    /jsonb_typeof\(v_query->'cursor'\) = 'null'/,
  );
  assert.match(
    queryValidation,
    /jsonb_typeof\(v_facet_request->'search'\) <> 'null'/,
  );
  assert.match(triggerInstaller, /settings_defaults/);
  assert.match(triggerInstaller, /candidates/);
  assert.match(triggerInstaller, /bump_generate/);
  assert.match(triggerInstaller, /bump_issue/);
  assert.match(
    historicalTriggerMigration,
    /deferred to post-helper repeatable/i,
  );
});

test('manifest carriers remain hidden until bounded release', () => {
  const migration = read(
    'supabase/migrations/27072026_1042_01_invoice_async_v8_workflow_columns.sql',
  );
  const manifest = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_18_private_invoice_batch_manifest_advance_v2.sql',
  );
  const claim = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_work_claim_batch.sql',
  );

  assert.match(migration, /result_visible boolean not null default false/i);
  assert.match(manifest, /'WAITING_MANIFEST_COMMIT'/);
  assert.match(manifest, /result_visible,\s*created_at_utc/i);
  assert.match(manifest, /true,\s*false,\s*false,\s*v_now,\s*v_now/);
  assert.match(manifest, /limit 250/i);
  assert.match(manifest, /'RELEASE_MANIFEST'/);
  assert.match(claim, /c\.manifest_committed/);
  assert.match(claim, /o\.manifest_committed/);
});

test('result paging is direct, bounded, and revision-gated', () => {
  const operationGet = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_get.sql',
  );
  const resultRevision = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_06_private_invoice_result_page_revision_trigger_v2.sql',
  );

  assert.doesNotMatch(operationGet, /_invoice_current_chunks_batch\s*\(/);
  assert.match(operationGet, /result_page_revision/);
  assert.match(
    operationGet,
    /least\(\(p_page_request->>'limit'\)::integer,100\)/,
  );
  assert.match(operationGet, /OPERATION_RESULT_CURSOR_STALE/);
  assert.match(
    resultRevision,
    /nextval\('public\.invoice_operation_change_seq'::regclass\)/,
  );
});

test('batch operation control does not preload a capped current-chunk graph', () => {
  const operationControl = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_control_batch.sql',
  );

  assert.doesNotMatch(operationControl, /_invoice_current_chunks_batch\s*\(/);
  assert.match(operationControl, /chunk_chain\(/);
  assert.doesNotMatch(operationControl, /null,null,10000/);
});

test('committed TEST configuration keeps both async entry flags disabled', () => {
  const wrangler = read('wrangler.toml');
  const pipelineFlags = [
    ...wrangler.matchAll(/INVOICE_ASYNC_PIPELINE_ENABLED\s*=\s*"([^"]+)"/g),
  ];
  const scheduledFlags = [
    ...wrangler.matchAll(/INVOICE_ASYNC_SCHEDULED_ENABLED\s*=\s*"([^"]+)"/g),
  ];

  assert.ok(pipelineFlags.length > 0);
  assert.ok(scheduledFlags.length > 0);
  assert.ok(pipelineFlags.every(match => match[1] === 'false'));
  assert.ok(scheduledFlags.every(match => match[1] === 'false'));
});
