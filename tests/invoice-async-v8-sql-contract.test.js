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
  ).filter(name => name.startsWith('27072026_1042_'));
  const repeatableFiles = readdirSync(v8RepeatableDirectory);

  assert.equal(migrationFiles.length, 4);
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
    'GROUP_KEY',
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
  assert.match(rules, /jsonb_array_length\(v_rules\) > 10000/);
  assert.match(rules, /BATCH_SELECTION_SELECTOR_INVALID/);
  for (const candidateSql of [generate, issue]) {
    assert.match(candidateSql, /v_mode='EXPLICIT_KEYS'/);
    assert.match(candidateSql, /jsonb_array_length\(v_selection_keys\) > 100/);
    assert.match(candidateSql, /BATCH_SOURCE_CHANGED/);
    assert.match(candidateSql, /facet_client_rows as materialized/i);
    assert.match(candidateSql, /facet_blocker_rows as materialized/i);
  }
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
