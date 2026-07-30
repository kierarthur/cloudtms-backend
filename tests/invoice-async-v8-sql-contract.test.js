import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
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
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_dispatch_advance_batch.sql',
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_resolve_command_groups.sql',
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
    'DIMENSION_GROUP',
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

test('SUMMARY group selectors are canonical, bounded, and authoritative', () => {
  const validator = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1250_08a_private_invoice_batch_query_validate_v2.sql',
  );
  const generate = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_09_private_invoice_batch_generate_candidate_rows_v2.sql',
  );
  const issue = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_10_private_invoice_batch_issue_candidate_rows_v2.sql',
  );

  assert.match(validator, /jsonb_array_length\(v_query->'group_selectors'\) > 400/);
  assert.match(validator, /_invoice_batch_selection_rules_v2\s*\(/);
  assert.match(validator, /count\(distinct selector\.value\)/);
  assert.match(validator, /BATCH_SELECTION_SELECTOR_INVALID/);
  assert.match(validator, /'group_selectors', v_normalized_group_selectors/);

  for (const candidateSql of [generate, issue]) {
    assert.match(candidateSql, /requested_group_selectors as materialized/i);
    assert.match(candidateSql, /requested_group_members as materialized/i);
    assert.match(candidateSql, /requested_group_base as materialized/i);
    assert.match(candidateSql, /requested_group_rollup as materialized/i);
    assert.match(candidateSql, /summary_group_selection_json as materialized/i);
    assert.match(candidateSql, /member\.last_selection_action is distinct from requested\.base_action/i);
    assert.match(candidateSql, /count\(distinct member\.group_key\)=1/i);
    assert.match(candidateSql, /when v_mode='SUMMARY' then summary_groups\.groups/i);
    assert.match(candidateSql, /order by rollup\.request_ordinal/i);
    assert.match(
      candidateSql,
      /rollup\.selected_total=rollup\.eligible_total[\s\S]*not rollup\.has_hidden_override then 'CHECKED'/i,
    );
    assert.match(
      candidateSql,
      /requested\.selector_type='CANDIDATE'[\s\S]*jsonb_array_elements_text[\s\S]*candidate\.value::uuid=requested\.candidate_id/i,
    );
    assert.match(
      candidateSql,
      /requested\.selector_type='WEEK_CLIENT_CANDIDATE'[\s\S]*candidate\.value::uuid=requested\.candidate_id/i,
    );
    assert.match(
      candidateSql,
      /requested\.selector_type='DIMENSION_GROUP'[\s\S]*requested\.candidate_id is null[\s\S]*candidate\.value::uuid=requested\.candidate_id/i,
    );
    assert.doesNotMatch(
      candidateSql,
      /summary_group_selection_json[\s\S]*_invoice_batch_(?:generate_group_rows|issue_source_rows_for_ids)_v2\s*\(/i,
    );
  }
});

test('timesheet presentation uses authoritative schedule and authorisation fields', () => {
  const presentation = read(
    'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql',
  );

  assert.match(presentation, /'authorised',t\.authorised_at_server is not null/g);
  assert.match(presentation, /extract\(epoch from \(/g);
  assert.match(presentation, /s\.value->>'start_utc'/g);
  assert.match(presentation, /s\.value->>'end_utc'/g);
  assert.match(presentation, /s\.value->>'break_mins'/g);
  assert.match(presentation, /t\.shift_label_norm !~\* '\^weekly-correction-'/g);
  assert.match(presentation, /length\(t\.shift_label_norm\) <= 80/g);
  assert.doesNotMatch(
    presentation,
    /'authorisation',jsonb_build_object\('authorised',t\.auth_name is not null/,
  );
  assert.match(
    presentation,
    /row_number\(\) over\(order by rs\.day_ymd,rs\.row_key\)::integer as display_order/i,
  );
  assert.match(
    presentation,
    /row_number\(\) over\(order by rs\.segment_id,rs\.row_key\)::integer as display_order/i,
  );
  assert.doesNotMatch(
    presentation,
    /'display_order',rs\.row_key/i,
  );
});

test('Batch Generate creates invoice records only while Batch Issue accepts document preparation', () => {
  const generateKeys = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1501_private_invoice_batch_generate_candidate_keys_v2.sql',
  );
  const generateRows = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_09_private_invoice_batch_generate_candidate_rows_v2.sql',
  );
  const generationAdvance = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
  );
  const issueClassification = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1806_private_invoice_batch_issue_classification_v2.sql',
  );
  const issueValidation = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_issue_validate_batch.sql',
  );
  assert.match(generateKeys, /candidate\.candidate_json->>'row_kind'='CREATE_INVOICE'/i);
  assert.match(generateKeys, /SEGMENT_ALREADY_LOCKED/i);
  assert.match(generateKeys, /SOURCE_ALREADY_LOCKED/i);
  assert.match(generateKeys, /SOURCE_ALREADY_INVOICED/i);
  assert.match(generateRows, /not in\s*\([\s\S]*'SEGMENT_ALREADY_LOCKED'[\s\S]*'SOURCE_ALREADY_LOCKED'/i);
  assert.match(generateRows, /blocker_detail,code[\s\S]*SOURCE_ALREADY_INVOICED[\s\S]*SEGMENT_ALREADY_LOCKED[\s\S]*SOURCE_ALREADY_LOCKED/i);
  assert.match(generateRows, /jsonb_array_elements_text[\s\S]*blocker_codes[\s\S]*SOURCE_ALREADY_INVOICED[\s\S]*SEGMENT_ALREADY_LOCKED[\s\S]*SOURCE_ALREADY_LOCKED/i);
  assert.match(generateRows, /jsonb_array_elements[\s\S]*blocker_detail,sources[\s\S]*SOURCE_ALREADY_INVOICED[\s\S]*SEGMENT_ALREADY_LOCKED[\s\S]*SOURCE_ALREADY_LOCKED/i);
  assert.match(
    generateRows,
    /authoritative_rows[\s\S]*_invoice_batch_generate_classification_v2[\s\S]*row_kind'='CREATE_INVOICE'[\s\S]*candidate_keys[\s\S]*page_ordinal<=v_page_size[\s\S]*primary_blocker_code[\s\S]*SOURCE_ALREADY_INVOICED[\s\S]*SEGMENT_ALREADY_LOCKED[\s\S]*SOURCE_ALREADY_LOCKED[\s\S]*action_blocker_codes/i
  );
  assert.match(
    generationAdvance,
    /set status=case[\s\S]*then 'COMPLETE' else 'BLOCKED' end,[\s\S]*phase=case[\s\S]*then 'COMPLETE' else 'BLOCKED' end/i,
  );
  assert.match(
    generationAdvance,
    /tf\.basis::text in\('NHSP','NHSP_ADJUSTMENT'\)[\s\S]*then 'NHSP'/i,
  );
  assert.match(
    generationAdvance,
    /with recursive claim_ids as materialized[\s\S]*source_timesheet_ancestry[\s\S]*parent_timesheet_id[\s\S]*ancestry_depth<32/i,
  );
  assert.match(
    generationAdvance,
    /adjustment_segment_refs[\s\S]*basis::text='NHSP_ADJUSTMENT'[\s\S]*btrim\(coalesce\(n\.ref_num,''\)\)=r\.ref_num/i,
  );
  assert.match(
    generationAdvance,
    /reversal_row_keys[\s\S]*jsonb_build_object\('reversal_state','REVERSED'\)/i,
  );
  assert.match(
    issueValidation,
    /import_source_requirements as materialized[\s\S]*invoice_hr_source_rows[\s\S]*jsonb_array_length\(source\.rows_json\)>0/i,
  );
  assert.match(issueValidation, /MISSING_IMPORT_SOURCE_EVIDENCE/i);
  assert.match(issueValidation, /missing_import_source_count/i);
  assert.doesNotMatch(issueClassification, /generated_state='FRESH'/i);
  assert.match(
    issueClassification,
    /\(\s*classified\.hard_blocker_codes\s*\)\s+issue_blocker_codes/i,
  );
  assert.doesNotMatch(
    issueClassification,
    /generated_state\s*(?:=|in)\s*\([^)]*\)[\s\S]{0,240}issue_blocker_codes/i,
  );
});

test('invoice presentation consolidates import support and exposes complete commercial line columns', () => {
  const presentation = read(
    'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql',
  );
  assert.match(presentation, /select distinct s\.invoice_id,upper\(s\.source_system\) source_system/i);
  assert.match(presentation, /'import_ids',coalesce/i);
  assert.match(presentation, /'worker',c\.worker_name/i);
  assert.match(presentation, /'terms_text',coalesce/i);
  assert.match(presentation, /'reversal_state',case/i);
  assert.match(presentation, /order by[\s\S]*shift_date[\s\S]*s\.import_id[\s\S]*r\.ordinality/i);
});

test('invoice presentation maps authoritative NHSP import fields into readable support rows', () => {
  const presentation = read(
    'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql',
  );
  assert.match(presentation, /r\.value->>'worker_name'/i);
  assert.match(presentation, /r\.value->>'staff_name'/i);
  assert.match(presentation, /r\.value->>'unique_id'/i);
  assert.match(presentation, /r\.value->>'ref_num'/i);
  assert.match(presentation, /r\.value->>'assignment_code'/i);
  assert.match(presentation, /r\.value->>'work_date'/i);
  assert.match(presentation, /r\.value->>'date_raw'/i);
  assert.match(presentation, /r\.value->>'start_local'/i);
  assert.match(presentation, /r\.value->>'end_local'/i);
  assert.match(presentation, /r\.value->>'hours_worked'/i);
  assert.match(
    presentation,
    /'source_identity',concat_ws\(' · ',[\s\S]*'import row '\|\|r\.ordinality::text\)/i,
  );
  assert.match(
    presentation,
    /'commission_amount',coalesce\([\s\S]*lower\(btrim\(h\.header_name\)\)='commission'/i,
  );
  assert.match(
    presentation,
    /'total_amount',coalesce\([\s\S]*lower\(btrim\(h\.header_name\)\) in \('total cost','total amount'\)/i,
  );
  assert.equal(
    (presentation.match(/->>\(\(h\.ordinality-1\)::integer\)/gi) || []).length,
    2,
    'NHSP raw-column lookups must use an integer JSON array index at runtime',
  );
  assert.match(
    presentation,
    /'suppress_candidate_header',[\s\S]*upper\(nhsp\.source_system\)='NHSP'[\s\S]*count\(distinct il\.timesheet_id\)[\s\S]*count\(distinct nullif\(btrim\(lb\.worker_name\),''\)\)/i,
  );
  assert.doesNotMatch(
    presentation,
    /'source_identity',jsonb_build_object\('source_system'/i,
  );
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
  assert.match(snapshotGet, /'key_id',\s*v_key_id/i);
  assert.match(snapshotVerify, /BATCH_SNAPSHOT_SIGNATURE_INVALID/);
  assert.match(snapshotVerify, /BATCH_SNAPSHOT_CHANGED/);
  assert.match(
    snapshotVerify,
    /v_parts\[2\]\s+is\s+distinct\s+from\s+v_key_id/i,
  );
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
  assert.ok(vectors.vectors.length >= 11);
  for (const vector of vectors.vectors) {
    assert.match(vector.sha256, /^[0-9a-f]{64}$/);
    assert.equal(
      createHash('sha256').update(vector.canonical_text).digest('hex'),
      vector.sha256,
    );
  }

  assert.equal(
    triggerManifest.contract_version,
    'INVOICE_ASYNC_TRIGGER_MANIFEST_V2',
  );
  assert.equal(triggerManifest.table_count, 18);
  assert.equal(triggerManifest.candidate_trigger_count, 54);
  assert.equal(triggerManifest.result_trigger_count, 3);
  assert.equal(triggerManifest.tables.length, 18);
  assert.match(
    read(
      'supabase/repeatable/27072026_1042_invoice_async_v8/'
        + '27072026_1042_19_invoice_async_contract_get_v2.sql',
    ),
    new RegExp(triggerManifest.manifest_digest),
  );
  for (const table of triggerManifest.tables) {
    assert.ok(table.field_count > 0);
    assert.ok(Array.isArray(table.fields));
    assert.equal(table.fields.length, table.field_count);
    assert.ok(Array.isArray(table.json_paths));
    if (['invoice_operations', 'invoice_operation_chunks'].includes(
      table.table_name,
    )) {
      assert.ok(!table.fields.includes('input_json'));
      assert.ok(!table.fields.includes('progress_json'));
      assert.ok(!table.fields.includes('result_json'));
      assert.ok(!table.fields.includes('payload_json'));
      assert.ok(!table.fields.includes('error_json'));
    }
    assert.match(table.projection_sha256, /^[0-9a-f]{64}$/);
  }

  assert.equal(
    functionHashes.contract_version,
    'INVOICE_ASYNC_DB_V2_FUNCTION_HASH_MANIFEST',
  );
  assert.equal(functionHashes.functions.length, 40);
  assert.match(functionHashes.aggregate_sha256, /^[0-9a-f]{64}$/);
  const hashedIdentities = new Set(
    functionHashes.functions.map(fn => fn.identity),
  );
  for (const identity of [
    'private._invoice_batch_generate_candidate_keys_v2(jsonb,timestamptz)',
    'private._invoice_batch_generate_classification_v2(boolean,text[],timestamptz)',
    'private._invoice_batch_issue_candidate_keys_v2(jsonb,timestamptz)',
    'private._invoice_batch_issue_classification_v2(boolean,uuid[],timestamptz)',
    'private._invoice_batch_issue_source_rows_core_v2(boolean,integer,timestamptz,uuid[])',
    'private._invoice_batch_issue_source_rows_for_ids_v2(uuid[],boolean,timestamptz)',
    'private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz)',
    'private._invoice_dispatch_advance_batch(jsonb,timestamptz)',
  ]) {
    assert.ok(hashedIdentities.has(identity), identity);
  }
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
  const contractProbe = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_19_invoice_async_contract_get_v2.sql',
  );
  const historicalTriggerMigration = read(
    'supabase/migrations/27072026_1042_04_invoice_async_v8_candidate_revision_triggers.sql',
  );
  const legacyGenerateWrapper = read(
    'supabase/repeatable/24072026_1646_invoice_batch_generate_candidates.sql',
  );
  const legacyIssueWrapper = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_batch_issue_candidates.sql',
  );
  const legacyOverloadMigration = read(
    'supabase/migrations/28072026_0958_invoice_candidate_rpc_overload_defaults.sql',
  );

  assert.match(queryValidation, /INVOICE_BATCH_QUERY_UNKNOWN_FIELD/);
  assert.match(queryValidation, /INVOICE_BATCH_QUERY_ACTION_REQUIRED/);
  assert.match(queryValidation, /INVOICE_BATCH_QUERY_ACTION_MISMATCH/);
  assert.match(queryValidation, /INVOICE_BATCH_QUERY_MODE_FIELD_INVALID/);
  assert.match(queryValidation, /v_mode = 'PAGE'[\s\S]*> 100/);
  assert.match(queryValidation, /v_mode = 'EXPAND_SELECTION'[\s\S]*> 250/);
  assert.doesNotMatch(queryValidation, /least\([^)]*page_size/i);
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
  assert.match(contractProbe, /legacy_runtime_exposure_count/);
  assert.match(contractProbe, /legacy_rest_overload_ambiguity_count/);
  assert.match(contractProbe, /pronargdefaults/);
  assert.match(
    contractProbe,
    /legacy\.procedure_identity is not null[\s\S]*coalesce\(p\.pronargdefaults, 0\) <> 0/,
  );
  assert.doesNotMatch(
    contractProbe,
    /legacy\.procedure_identity is null[\s\S]*coalesce\(p\.pronargdefaults, 0\) <> 0/,
  );
  assert.match(
    contractProbe,
    /legacy_surface_state\.legacy_rest_overload_ambiguity_count=0/,
  );
  assert.match(
    contractProbe,
    /invoice_batch_generate_candidates\(boolean,integer,text\[\],jsonb\)/,
  );
  assert.match(
    contractProbe,
    /invoice_batch_issue_candidates\(boolean,integer,jsonb\)/,
  );
  assert.doesNotMatch(
    legacyGenerateWrapper.match(
      /CREATE OR REPLACE FUNCTION public\.invoice_batch_generate_candidates\([\s\S]*?\)\s*RETURNS/,
    )?.[0] || '',
    /\bDEFAULT\b/i,
  );
  assert.doesNotMatch(
    legacyIssueWrapper.match(
      /CREATE OR REPLACE FUNCTION public\.invoice_batch_issue_candidates\([\s\S]*?\)\s*RETURNS/,
    )?.[0] || '',
    /\bDEFAULT\b/i,
  );
  assert.match(
    legacyOverloadMigration,
    /drop function if exists public\.invoice_batch_generate_candidates\s*\(\s*boolean,\s*integer,\s*text\[\],\s*jsonb\s*\)/i,
  );
  assert.match(
    legacyOverloadMigration,
    /drop function if exists public\.invoice_batch_issue_candidates\s*\(\s*boolean,\s*integer,\s*jsonb\s*\)/i,
  );
  assert.doesNotMatch(
    legacyOverloadMigration.replace(/--.*$/gm, ''),
    /create\s+(?:or\s+replace\s+)?function/i,
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
  assert.match(manifest, /'AWAITING_MANIFEST_COMMIT'/);
  assert.match(manifest, /result_visible,\s*created_at_utc/i);
  assert.match(manifest, /true,\s*false,\s*false,\s*v_now,\s*v_now/);
  assert.match(manifest, /limit 250/i);
  assert.match(manifest, /'RELEASE_MANIFEST'/);
  assert.match(manifest, /'AWAITING_RELEASE'/);
  assert.match(claim, /c\.manifest_committed/);
  assert.match(claim, /o\.manifest_committed/);
  assert.match(
    claim,
    /c\.phase in \('BUILD_MANIFEST','RELEASE_MANIFEST'\)/,
  );
  assert.match(claim, /'AWAITING_MANIFEST_COMMIT'/);
  assert.match(claim, /'AWAITING_RELEASE'/);
});

test('direct Issue and Delivery carriers are committed claimable members', () => {
  const startCore = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_11_private_invoice_operation_start_core_v8.sql',
  );
  const claim = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_work_claim_batch.sql',
  );

  assert.match(
    startCore,
    /manifest_generation,\s*is_manifest_member,\s*manifest_committed,\s*result_visible/i,
  );
  assert.match(
    startCore,
    /change_seq,\s*manifest_generation,\s*manifest_committed,\s*release_complete/i,
  );
  assert.equal(
    (
      startCore.match(
        /when [sc]\.command_type in \('ISSUE_INVOICES','DELIVER_INVOICES'\)\s*then 1 else 0 end/g,
      ) || []
    ).length,
    1,
  );
  assert.equal(
    (
      startCore.match(
        /when c\.chunk_type in \('ISSUE_INVOICE','DELIVERY_PREPARE'\)\s*then 1 else 0 end/g,
      ) || []
    ).length,
    1,
  );
  assert.equal(
    (
      startCore.match(
        /c\.chunk_type in \('ISSUE_INVOICE','DELIVERY_PREPARE'\)/g,
      ) || []
    ).length,
    3,
  );
  assert.match(
    claim,
    /c\.is_manifest_member[\s\S]*o\.manifest_committed[\s\S]*c\.manifest_committed/i,
  );
});

test('direct Delivery carriers preserve routing and bind request correlation to the chunk', () => {
  const startCore = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_11_private_invoice_operation_start_core_v8.sql',
  );
  const deliveryAdvance = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_delivery_advance_batch.sql',
  );

  assert.match(
    startCore,
    /'routing_request',jsonb_build_object\([\s\S]*?'recipient_set',rs\.canonical_recipients[\s\S]*?'delivery_policy'[\s\S]*?'template_version'/i,
  );
  assert.match(
    startCore,
    /'frozen_delivery_route',jsonb_build_object\([\s\S]*?'delivery_suppressed',dr\.delivery_suppressed[\s\S]*?'template_version'[\s\S]*?'delivery_policy'/i,
  );
  assert.match(
    startCore,
    /insert into public\.invoice_operation_chunks\(\s*id,operation_id[\s\S]*?case when c\.chunk_type='DELIVERY_PREPARE'[\s\S]*?'\{request_key\}'[\s\S]*?to_jsonb\(generated\.id::text\)[\s\S]*?'\{routing_request,request_key\}'[\s\S]*?'\{frozen_delivery_route,request_key\}'/i,
  );
  assert.match(
    startCore,
    /cross join lateral \(\s*select gen_random_uuid\(\) id\s*where c\.operation_id is not null\s*\) generated/i,
  );
  assert.match(
    deliveryAdvance,
    /when f\.request_key is distinct from f\.chunk_id::text\s*then 'REQUEST_CORRELATION_INVALID'/i,
  );
  assert.match(
    deliveryAdvance,
    /r\.routing_request->'recipient_set'/i,
  );
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
  assert.match(resultRevision, /for update;/i);
  assert.match(
    resultRevision,
    /nextval\(\s*'public\.invoice_operation_change_seq'::regclass\s*\)/,
  );
  assert.match(
    resultRevision,
    /result_page_revision\s*=\s*greatest\(/i,
  );
  assert.match(
    resultRevision,
    /change_seq\s*=\s*greatest\(/i,
  );
});

test('batch operation control does not preload a capped current-chunk graph', () => {
  const operationControl = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_control_batch.sql',
  );

  assert.doesNotMatch(operationControl, /_invoice_current_chunks_batch\s*\(/);
  assert.match(operationControl, /chunk_chain\(/);
  assert.doesNotMatch(operationControl, /null,null,10000/);
  assert.match(operationControl, /BATCH_TARGETED_RETRY_REQUIRED/);
  assert.match(
    operationControl,
    /o\.status='WAITING'[\s\S]*?o\.requires_user_action[\s\S]*?retryable_chunk\.replaced_by_chunk_id is null[\s\S]*?retryable_chunk\.status in\([\s\S]*?'FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'/i,
  );
  assert.match(operationControl, /depth<64/);
  assert.match(
    operationControl,
    /chunk_chain\([\s\S]*?select\s+requested\.request_no,/i,
  );
  assert.doesNotMatch(
    operationControl,
    /chunk_chain\([\s\S]*?select\s+tree\.request_no,/i,
  );
  assert.match(
    operationControl,
    /v_now\s+timestamptz\s*:=\s*statement_timestamp\(\)/i,
  );
  assert.match(
    operationControl,
    /if v_jwt_role='service_role' then[\s\S]*coalesce\(p_now_utc,statement_timestamp\(\)\)/i,
  );
  assert.doesNotMatch(
    operationControl,
    /v_now\s+timestamptz\s*:=\s*coalesce\(p_now_utc/i,
  );
});

test('operation-control retry installer references the single canonical function authority', () => {
  const installer = read(
    'supabase/repeatable/29072026_1523_invoice_operation_control_retry_waiting.sql',
  );
  assert.match(installer, /^\\set ON_ERROR_STOP on/m);
  assert.match(installer, /\bbegin;/i);
  assert.match(
    installer,
    /\\ir 23072026_2207_invoice_queue_stage1_revision8\/23072026_2207_invoice_operation_control_batch\.sql/,
  );
  assert.match(installer, /\bcommit;/i);
  assert.doesNotMatch(installer, /create\s+or\s+replace\s+function/i);
});

test('operation control replacement hashing uses the fixed extensions schema', () => {
  const operationControl = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_control_batch.sql',
  );

  assert.match(operationControl, /extensions\.digest\s*\(/i);
  assert.doesNotMatch(operationControl, /(?<!\.)\bdigest\s*\(/i);
});

test('batch rollup qualifies output-column names inside SQL statements', () => {
  const rollup = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_operation_rollup_batch.sql',
  );

  assert.doesNotMatch(rollup, /select\s+operation_id\s+from\s+roots/i);
  assert.match(
    rollup,
    /from\s+unnest\(v_root_operation_ids\)\s+root_id/gi,
  );
  assert.doesNotMatch(
    rollup,
    /descendants\s*\(\s*root_operation_id\s*,/i,
  );
  assert.match(
    rollup,
    /descendants\s*\(\s*descendant_root_operation_id\s*,/i,
  );
  assert.doesNotMatch(
    rollup,
    /progress\s+as\s+materialized\s*\(\s*select\s+counts\.batch_root_id\s*,/i,
  );
});

test('operation control has durable database idempotency receipts', () => {
  const operationControl = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_control_batch.sql',
  );
  const migration = read(
    'supabase/migrations/27072026_2321_invoice_operation_control_idempotency_receipts.sql',
  );
  const contractProbe = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_19_invoice_async_contract_get_v2.sql',
  );

  assert.match(operationControl, /INVOICE_OPERATION_CONTROL_V2/);
  assert.match(operationControl, /OPERATION_CONTROL_REQUEST_TOKEN_REQUIRED/);
  assert.match(operationControl, /OPERATION_CONTROL_REQUEST_HASH_MISMATCH/);
  assert.match(operationControl, /OPERATION_CONTROL_IDEMPOTENCY_CONFLICT/);
  assert.match(operationControl, /OPERATION_CONTROL_IDEMPOTENCY_EXPIRED/);
  assert.match(operationControl, /OPERATION_CONTROL_RECEIPT_IMMUTABLE/);
  assert.match(operationControl, /pg_advisory_xact_lock\s*\(/i);
  assert.match(operationControl, /private\._invoice_batch_hash_v2\s*\(/i);
  assert.match(operationControl, /'logical_result',v_result/i);
  assert.match(operationControl, /interval '30 days'/i);
  assert.match(migration, /'OPERATION_CONTROL_REQUEST'/);
  assert.match(
    migration,
    /idx_invoice_operation_control_receipt_actor_token_v8/,
  );
  assert.doesNotMatch(migration, /create\s+table/i);
  assert.match(
    contractProbe,
    /idx_invoice_operation_control_receipt_actor_token_v8/,
  );
});

test('operation-control receipts are excluded from candidate revision authority', () => {
  const trigger = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_05_private_invoice_candidate_revision_trigger_v2.sql',
  );

  assert.match(
    trigger,
    /coalesce\(operation_type,\s*''\)\s*<>\s*'OPERATION_CONTROL_REQUEST'/i,
  );
  assert.match(
    trigger,
    /full\s+join\s+new_rows\s+n\s+on\s+n\.id\s*=\s*o\.id/i,
  );
  assert.match(
    trigger,
    /o\.id\s+is\s+not\s+null[\s\S]*OPERATION_CONTROL_REQUEST[\s\S]*<>\s*\([\s\S]*n\.id\s+is\s+not\s+null[\s\S]*OPERATION_CONTROL_REQUEST/i,
  );
  assert.match(
    trigger,
    /o\.id\s+is\s+not\s+null[\s\S]*n\.id\s+is\s+not\s+null[\s\S]*jsonb_build_object\(/i,
  );
  assert.doesNotMatch(
    trigger,
    /coalesce\(entity_type,\s*''\)\s*<>\s*'OPERATION_CONTROL'/i,
  );
});

test('candidate PAGE selects bounded keys before expensive hydration', () => {
  const generateKeys = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1501_private_invoice_batch_generate_candidate_keys_v2.sql',
  );
  const issueKeys = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1501_private_invoice_batch_issue_candidate_keys_v2.sql',
  );
  const generateRows = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_09_private_invoice_batch_generate_candidate_rows_v2.sql',
  );
  const issueRows = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_10_private_invoice_batch_issue_candidate_rows_v2.sql',
  );
  const generateResolver = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_resolve_command_groups.sql',
  );
  const issueSourcesForIds = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1501_private_invoice_batch_issue_source_rows_for_ids_v2.sql',
  );
  const issueSources = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_10a_private_invoice_batch_issue_source_rows_v2.sql',
  );
  const generateClassifier = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1806_private_invoice_batch_generate_classification_v2.sql',
  );
  const issueClassifier = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1806_private_invoice_batch_issue_classification_v2.sql',
  );

  assert.doesNotMatch(
    generateKeys,
    /_invoice_batch_generate_group_rows_v2\s*\(/,
  );
  assert.doesNotMatch(
    generateKeys,
    /public\.invoice_batch_generate_candidates\s*\(/,
  );
  assert.doesNotMatch(
    issueKeys,
    /_invoice_batch_issue_source_rows_core_v2\s*\(/,
  );
  assert.doesNotMatch(
    issueKeys,
    /public\.invoice_batch_issue_candidates\s*\(/,
  );
  assert.match(
    generateKeys,
    /_invoice_batch_generate_classification_v2\s*\(/,
  );
  assert.match(
    issueKeys,
    /_invoice_batch_issue_classification_v2\s*\(/,
  );
  assert.match(generateKeys, /scope_count as materialized/i);
  assert.match(issueKeys, /scope_count as materialized/i);
  assert.ok(
    generateKeys.indexOf('scope_count as materialized')
      < generateKeys.indexOf('cursor_filtered as materialized'),
  );
  assert.ok(
    issueKeys.indexOf('scope_count as materialized')
      < issueKeys.indexOf('cursor_filtered as materialized'),
  );
  assert.doesNotMatch(
    generateKeys,
    /client_setting\.vat_rate_pct|finance\.vat_rate_pct/,
  );
  assert.match(
    generateClassifier,
    /_invoice_generation_vat_policy_batch\s*\(/,
  );
  assert.match(
    generateClassifier,
    /_invoice_correction_validate_batch\s*\(/,
  );
  assert.match(
    issueClassifier,
    /_invoice_issue_validate_batch\s*\(/,
  );
  assert.doesNotMatch(
    issueSources,
    /_invoice_issue_validate_batch\s*\(/,
  );
  assert.match(
    generateRows,
    /_invoice_batch_generate_candidate_keys_v2\s*\(/,
  );
  assert.match(
    generateRows,
    /page_ordinal<=v_page_size/,
  );
  assert.match(
    issueRows,
    /_invoice_batch_issue_candidate_keys_v2\s*\(/,
  );
  assert.match(
    issueRows,
    /_invoice_batch_issue_source_rows_for_ids_v2\s*\(/,
  );
  assert.doesNotMatch(
    generateRows,
    /v_requires_exact_scope/,
  );
  assert.doesNotMatch(
    issueRows,
    /v_requires_exact_scope/,
  );
  assert.match(
    generateRows,
    /when v_mode in \('FACETS','SUMMARY'\) then '\{\}'::text\[\][\s\S]*_invoice_batch_generate_group_rows_v2\s*\([\s\S]*request\.scope_keys[\s\S]*where v_mode in \('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS'\)[\s\S]*cardinality\(request\.scope_keys\)>0/,
  );
  assert.match(
    issueRows,
    /when v_mode in \('FACETS','SUMMARY'\) then '\{\}'::uuid\[\][\s\S]*_invoice_batch_issue_source_rows_for_ids_v2\s*\([\s\S]*request\.invoice_ids[\s\S]*where v_mode in \('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS'\)[\s\S]*cardinality\(request\.invoice_ids\)>0/,
  );
  assert.doesNotMatch(
    issueRows,
    /_invoice_batch_issue_source_rows_v2\s*\(/,
  );
  assert.match(
    generateKeys,
    /selection_rules as materialized[\s\S]*_invoice_batch_selection_rules_v2\s*\([\s\S]*last_selection_action[\s\S]*params\.mode<>'EXPAND_SELECTION'[\s\S]*classified\.selectable[\s\S]*classified\.last_selection_action<>'EXCLUDE'/,
  );
  assert.match(
    issueKeys,
    /selection_rules as materialized[\s\S]*_invoice_batch_selection_rules_v2\s*\([\s\S]*last_selection_action[\s\S]*params\.mode<>'EXPAND_SELECTION'[\s\S]*classified\.selectable[\s\S]*classified\.last_selection_action<>'EXCLUDE'/,
  );
  assert.match(
    generateRows,
    /v_mode<>'EXPAND_SELECTION'[\s\S]*r\.selectable[\s\S]*r\.last_selection_action<>'EXCLUDE'/,
  );
  assert.match(
    issueRows,
    /v_mode<>'EXPAND_SELECTION'[\s\S]*r\.selectable[\s\S]*r\.last_selection_action<>'EXCLUDE'/,
  );
  assert.match(
    generateRows,
    /'total_count',case[\s\S]*max\(k\.full_scope_count\)/,
  );
  assert.match(
    issueRows,
    /'total_count',case[\s\S]*max\(k\.full_scope_count\)/,
  );
  assert.match(
    generateRows,
    /_invoice_batch_generate_classification_v2\s*\(/,
  );
  assert.match(
    issueRows,
    /_invoice_batch_issue_classification_v2\s*\(/,
  );
  assert.match(
    generateKeys,
    /params\.display_mode='READY'[\s\S]*params\.display_mode='BLOCKED'/,
  );
  assert.match(
    issueKeys,
    /params\.display_mode='READY'[\s\S]*params\.display_mode='BLOCKED'/,
  );
  assert.match(
    generateKeys,
    /when params\.sort_key='STATUS'[\s\S]*row_status/,
  );
  assert.match(
    issueKeys,
    /when params\.sort_key='STATUS'[\s\S]*row_status/,
  );
  assert.ok(
    generateResolver.indexOf('source_rows_scoped as materialized')
      < generateResolver.indexOf('reference_results as materialized'),
  );
  assert.match(
    issueSourcesForIds,
    /_invoice_batch_issue_source_rows_for_ids_v2/,
  );
  assert.match(
    issueSourcesForIds,
    /cardinality\(p_invoice_ids\)>250/,
  );
  assert.match(
    issueClassifier,
    /p_invoice_ids is null[\s\S]*invoice\.id=any\(p_invoice_ids\)/,
  );
  assert.match(issueSources, /_invoice_batch_issue_classification_v2\s*\(/);
});

test('V8 rollup exposes the complete manifest and commercial contract', () => {
  const rollup = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_operation_rollup_batch.sql',
  );
  const operationStart = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_start_batch.sql',
  );
  const manifestAdvance = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_18_private_invoice_batch_manifest_advance_v2.sql',
  );
  for (const field of [
    'manifest_generation',
    'manifest_status',
    'manifest_committed',
    'expected_scan_total',
    'scanned_total',
    'selection_expansion_pending',
    'release_pending_total',
    'released_total',
    'release_conflict_total',
    'release_blocked_total',
    'release_complete',
    'committed_at_utc',
    'superseded_manifest_generation',
    'candidate_total',
    'invoice_total',
    'selected_total',
    'excluded_total',
    'expanded_total',
    'queued_total',
    'generated_total',
    'regenerated_total',
    'issued_total',
    'issued_send_blocked_total',
    'already_active_total',
    'blocked_total',
    'changed_total',
    'missing_total',
    'failed_total',
    'in_progress_total',
    'delivery_pending_total',
    'delivery_complete_total',
    'delivery_blocked_total',
  ]) {
    assert.match(rollup, new RegExp(`'${field}'`));
  }
  assert.doesNotMatch(
    rollup,
    /greatest\(\s*count\(c\.id\)::integer/i,
  );
  assert.match(
    rollup,
    /payload_json->>'manifest_outcome'='SELECTED'[\s\S]*expanded_total/i,
  );
  assert.match(
    rollup,
    /manifest_committed[\s\S]*status in\(\s*'QUEUED','RUNNING','WAITING','RETRY_WAIT'\s*\)[\s\S]*queued_total/i,
  );
  assert.match(operationStart, /'release_pending_total',0/i);
  assert.match(
    operationStart,
    /'excluded_total',\s*greatest\(v_eligible_total-v_selected_total,0\)/i,
  );
  assert.match(operationStart, /'missing_total',0/i);
  assert.match(
    manifestAdvance,
    /when result_row->>'code'='INVOICE_NOT_FOUND'[\s\S]*then 'MISSING'/i,
  );
});

test('Issue finalisation locks invoices deterministically and keeps delivery separate', () => {
  const issueCore = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_13_private_invoice_issue_advance_core_v8.sql',
  );

  assert.match(issueCore, /locked_invoices as materialized/);
  assert.match(issueCore, /order by invoice\.id\s*for update of invoice/i);
  assert.match(issueCore, /active_issue_operation_id=c\.operation_id/);
  assert.match(issueCore, /not d\.frozen_route_usable/);
  assert.match(issueCore, /'blocked_for_sending'/);
});

test('timesheet presentation uses authoritative schedule and authorisation fields', () => {
  const presentation = read(
    'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql',
  );

  assert.match(presentation, /'authorised',t\.authorised_at_server is not null/g);
  assert.match(presentation, /extract\(epoch from \(/g);
  assert.match(presentation, /s\.value->>'start_utc'/g);
  assert.match(presentation, /s\.value->>'end_utc'/g);
  assert.match(presentation, /s\.value->>'break_mins'/g);
  assert.match(presentation, /t\.shift_label_norm !~\* '\^weekly-correction-'/g);
  assert.match(presentation, /length\(t\.shift_label_norm\) <= 80/g);
  assert.doesNotMatch(
    presentation,
    /'authorisation',jsonb_build_object\('authorised',t\.auth_name is not null/,
  );
});

test('document manifest planning freezes snapshot and manifest in one version update', () => {
  const documentAdvance = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch.sql',
  );
  const versionUpdates = documentAdvance.match(
    /update public\.invoice_document_versions/gi,
  ) || [];

  assert.equal(versionUpdates.length, 1);
  assert.doesNotMatch(documentAdvance, /update_versions as materialized/i);
  assert.match(
    documentAdvance,
    /update_manifests as materialized[\s\S]*set snapshot_json=l\.snapshot_json_v5,[\s\S]*snapshot_hash=l\.snapshot_hash_v5,[\s\S]*manifest_json=mb\.manifest_json,[\s\S]*manifest_hash=mb\.manifest_hash,[\s\S]*status='WAITING_FOR_INPUTS'/i,
  );
  assert.match(
    documentAdvance,
    /from manifest_build mb[\s\S]*join linked l[\s\S]*l\.version_id=mb\.document_version_id/i,
  );
  assert.doesNotMatch(documentAdvance, /'ATTACHMENT_INDEX','DOCUMENT'/i);
  assert.doesNotMatch(documentAdvance, /'Attachment index'/i);
  assert.match(documentAdvance, /'TIMESHEET_RENDER_MODEL_V2'/i);
  assert.match(documentAdvance, /'HEALTHROSTER_PRESENTATION_V2'/i);
  assert.doesNotMatch(
    documentAdvance,
    /where exists\(select 1 from invoice_ts it where it\.chunk_id=l\.id and it\.attach_timesheet and not it\.no_timesheet_required\)/i,
  );
});

test('invoice source chunks keep the electronic timesheet template identity', () => {
  const documentAdvance = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch.sql',
  );
  const workComplete = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_invoice_work_complete_batch.sql',
  );

  assert.match(
    documentAdvance,
    /source_chunks as materialized[\s\S]*case when m\.input_type='ELECTRONIC_TIMESHEET'\s*then 'timesheet-professional-v2'\s*else coalesce\(l\.version_template_version,l\.template_version,''\)\s*end,'2'/i,
  );
  assert.match(
    documentAdvance,
    /'template_version',case when m\.input_type='ELECTRONIC_TIMESHEET'\s*then 'timesheet-professional-v2'\s*else coalesce\(l\.version_template_version,l\.template_version\)\s*end/i,
  );
  assert.match(
    workComplete,
    /coalesce\(i\.processor_result->>'template_version',''\)<>\s*case when i\.chunk_type='SOURCE_RENDER'\s*then coalesce\(i\.payload_json->>'template_version',''\)\s*else coalesce\(\(select v\.template_version[\s\S]*where v\.id=i\.document_version_id\),''\)\s*end/i,
  );
});

test('attachment-index render passes carry their frozen presentation identity', () => {
  const downstreamAdvance = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql',
  );
  const workComplete = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_invoice_work_complete_batch.sql',
  );

  assert.match(
    downstreamAdvance,
    /attachment_index_measure[\s\S]*'presentation_model_schema_version',[\s\S]*'ATTACHMENT_INDEX_PRESENTATION_V1'/i,
  );
  assert.match(
    downstreamAdvance,
    /'presentation_model_hash',encode\(digest\(jsonb_build_object\([\s\S]*'manifest_ordinal',a\.index_ordinal\)::text,'sha256'\),'hex'\)/i,
  );
  assert.match(
    downstreamAdvance,
    /'snapshot_hash',a\.snapshot_hash/i,
  );
  assert.match(
    downstreamAdvance,
    /'source_revision',a\.index_source_revision/i,
  );
  assert.match(
    downstreamAdvance,
    /attachment_metadata as materialized[\s\S]*v_timesheets_summary_base[\s\S]*supporting_sources[\s\S]*invoice_hr_source_rows/i,
  );
  assert.match(
    downstreamAdvance,
    /'worker',other\.payload_json->>'worker'[\s\S]*'week_or_date',other\.payload_json->>'week_or_date'[\s\S]*'reference',other\.payload_json->>'reference'/i,
  );
  assert.match(
    workComplete,
    /f\.payload_json\|\|jsonb_build_object\([\s\S]*'layout_phase','FINAL'/i,
  );
  assert.match(
    workComplete,
    /'row_id',coalesce\([\s\S]*display_row\.value->>'logical_source_key'[\s\S]*display_row\.value->>'row_id'/i,
  );
  assert.match(
    workComplete,
    /'attachment_number',coalesce\([\s\S]*display_row\.display_no::integer/i,
  );
  assert.match(
    workComplete,
    /'document_type',coalesce\([\s\S]*display_row\.value->>'label'[\s\S]*display_row\.value->>'input_type'/i,
  );
  assert.match(
    workComplete,
    /'expected_start_pages_hash',[\s\S]*encode\(digest\(f\.attachments::text,'sha256'\),'hex'\)/i,
  );
  assert.match(
    workComplete,
    /case when i\.chunk_type='SOURCE_RENDER'[\s\S]*then i\.payload_json->>'source_revision' end,[\s\S]*i\.asset_source_revision,i\.document_source_revision/i,
  );
});

test('document merge retry requeues the plan into the implemented wait-for-merge phase', () => {
  const operationControl = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_operation_control_batch.sql',
  );
  const documentAdvance = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql',
  );

  assert.doesNotMatch(operationControl, /PLAN_MERGES/);
  assert.match(
    operationControl,
    /phase=case when scope\.reset_inputs then 'WAIT_FOR_INPUTS' else 'WAIT_FOR_MERGE' end,[\s\S]*attempt_count=0,[\s\S]*run_after_utc=v_now/i,
  );
  assert.match(documentAdvance, /where x->>'phase'='WAIT_FOR_MERGE'/i);
});

test('merge and verify processor contexts carry frozen document identity', () => {
  const workContext = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_invoice_work_context_batch.sql'
  );
  const mergeContext = workContext.match(
    /when v\.chunk_type='PDF_MERGE' then jsonb_build_object\(([\s\S]*?)when v\.chunk_type='DOCUMENT_VERIFY'/
  )?.[1] || '';
  const verifyContext = workContext.match(
    /when v\.chunk_type='DOCUMENT_VERIFY' then jsonb_build_object\(([\s\S]*?)else '\{\}'::jsonb/
  )?.[1] || '';
  assert.match(mergeContext, /'source_revision',v\.document_source_revision/);
  assert.match(mergeContext, /'template_version',v\.template_version/);
  assert.match(verifyContext, /'source_revision',v\.document_source_revision/);
  assert.match(verifyContext, /'template_version',v\.template_version/);
});

test('presentation authority repeatable defers safely instead of failing CI while work is active', () => {
  const presentationSnapshot = read(
    'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql',
  );
  const presentationAuthority = read(
    'supabase/repeatable/25072026_0003_invoice_presentation_runtime_authority.sql',
  );
  assert.match(presentationSnapshot, /\\gset/i);
  assert.match(presentationSnapshot, /\\if :invoice_presentation_active_work/i);
  assert.match(
    presentationSnapshot,
    /INVOICE_PRESENTATION_SNAPSHOT_DEFERRED_ACTIVE_WORK/i,
  );
  assert.match(
    presentationSnapshot,
    /from public\.invoice_operation_chunks[\s\S]*status in \('QUEUED','RUNNING','WAITING','RETRY_WAIT'\)/i,
  );
  assert.doesNotMatch(
    presentationSnapshot,
    /invoice_presentation_active_work[\s\S]{0,240}'BLOCKED'/i,
  );
  assert.match(presentationAuthority, /\\gset/i);
  assert.match(presentationAuthority, /\\if :invoice_presentation_active_work/i);
  assert.match(
    presentationAuthority,
    /INVOICE_PRESENTATION_CUTOVER_DEFERRED_ACTIVE_WORK/i,
  );
  assert.match(
    presentationAuthority,
    /from public\.invoice_operation_chunks[\s\S]*status in \('QUEUED','RUNNING','WAITING','RETRY_WAIT'\)/i,
  );
  assert.doesNotMatch(
    presentationAuthority,
    /invoice_presentation_active_work[\s\S]{0,240}'BLOCKED'/i,
  );
  assert.doesNotMatch(
    presentationAuthority,
    /raise exception[\s\S]*INVOICE_PRESENTATION_CUTOVER_ACTIVE_WORK/i,
  );
  const completionInstall = presentationAuthority.indexOf(
    '24072026_1217_invoice_work_complete_batch.sql',
  );
  const activeWorkGuard = presentationAuthority.indexOf('select exists (');
  assert.ok(completionInstall >= 0 && completionInstall < activeWorkGuard);
  assert.equal(
    presentationAuthority.match(
      /24072026_1217_invoice_work_complete_batch\.sql/g,
    )?.length,
    1,
  );
  const manifestInstall = presentationAuthority.indexOf(
    '24072026_1217_private_invoice_document_advance_batch.sql',
  );
  assert.ok(manifestInstall >= 0 && manifestInstall < activeWorkGuard);
  assert.equal(
    presentationAuthority.match(
      /24072026_1217_private_invoice_document_advance_batch\.sql/g,
    )?.length,
    1,
  );
});

test('source render processor context carries the frozen source template identity', () => {
  const workContext = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_invoice_work_context_batch.sql'
  );
  const sourceContext = workContext.match(
    /when v\.chunk_type='SOURCE_RENDER' then jsonb_build_object\(([\s\S]*?)when v\.chunk_type='INVOICE_CORE_RENDER'/
  )?.[1] || '';
  assert.match(
    sourceContext,
    /'template_version',coalesce\([\s\S]*v\.payload_json->>'template_version',[\s\S]*sm\.frozen_model->>'template_version',[\s\S]*v\.template_version\)/,
  );
});

test('batch rollup separates carrier trigger writes from the root update', () => {
  const rollup = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_operation_rollup_batch.sql',
  );
  const returnQueryIndex = rollup.toLowerCase().indexOf('return query');
  assert.ok(returnQueryIndex > 0);

  const carrierWrites = rollup
    .slice(0, returnQueryIndex)
    .match(/update\s+public\.invoice_operation_chunks\s+carrier/gi) || [];
  assert.equal(carrierWrites.length, 2);
  assert.doesNotMatch(
    rollup.slice(returnQueryIndex),
    /\w+\s+as\s+materialized\s*\(\s*update\s+public\.invoice_operation_chunks/i,
  );
  assert.match(
    rollup.slice(returnQueryIndex),
    /updated_roots\s+as\s+materialized\s*\(\s*update\s+public\.invoice_operations\s+root/i,
  );
  assert.match(
    rollup,
    /when root\.status in \('FAILED','DEAD_LETTER'\)\s+and not p\.manifest_committed then root\.status/gi,
  );
  assert.match(
    rollup,
    /root\.status in \('FAILED','DEAD_LETTER'\)\s+or p\.failed_total\+p\.blocked_total\+p\.changed_total>0/i,
  );
});

test('generation resolves NHSP financials to the SELF_BILL invoice stream without losing their source family', () => {
  const resolver = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
    + '23072026_2207_private_invoice_generation_resolve_command_groups.sql',
  );

  assert.match(
    resolver,
    /when upper\(coalesce\(tf\.basis::text,''\)\) in\(\s*'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL',\s*'HEALTHROSTER_ADJUSTMENT'\s*\) then 'SELF_BILL'/s,
  );
  assert.doesNotMatch(
    resolver,
    /when upper\(coalesce\(tf\.basis::text,''\)\) like 'NHSP%' then 'NHSP'\s*when upper\(coalesce\(tf\.basis::text,''\)\) like 'HEALTHROSTER%' then 'HEALTHROSTER'\s*when coalesce\(parent_contract\.self_bill,contract\.self_bill,false\) then 'SELF_BILL'\s*else 'NORMAL'\s*end invoice_stream/s,
  );
  assert.match(
    resolver,
    /when upper\(coalesce\(tf\.basis::text,''\)\) like 'NHSP%' then 'NHSP'[\s\S]*end resolved_source_type/,
  );
});

test('candidate correction validation treats a planned invoice identity as null-safe', () => {
  const correctionValidation = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
    + '23072026_2207_private_invoice_correction_validate_batch.sql',
  );

  assert.match(
    correctionValidation,
    /left join line_scope ls\s+on ls\.invoice_id is not distinct from r\.invoice_id\s+and ls\.scope_key=r\.scope_key\s+and ls\.timesheet_id=r\.timesheet_id/s,
  );
  assert.doesNotMatch(
    correctionValidation,
    /left join line_scope ls using\(invoice_id,scope_key,timesheet_id\)/,
  );
});

test('generation validation accepts pending NHSP sources and resolves VAT from the source week', () => {
  const generationCore = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/'
    + '27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
  );

  assert.match(
    generationCore,
    /'effective_date',coalesce\([\s\S]*ts_vat\.week_ending_date::text[\s\S]*where ts_vat\.timesheet_id=m\.timesheet_id[\s\S]*and ts_vat\.is_current/s,
  );
  assert.match(
    generationCore,
    /ns_ready\.timesheet_id=m\.timesheet_id[\s\S]*ns_ready\.invoice_status='PENDING'[\s\S]*ns_ready\.invoice_id is null[\s\S]*ns_ready\.cancelled_at_utc is null/s,
  );
  assert.doesNotMatch(
    generationCore,
    /nhsp_shift_included_count,0\)=0/,
  );
});

test('generation commits exact NHSP shift ownership and attaches the authoritative import row', () => {
  const generationCore = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/'
    + '27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
  );

  assert.match(
    generationCore,
    /nhsp_shift_inclusion as \(\s*update public\.nhsp_shifts ns\s*set invoice_status='INCLUDED',\s*invoice_id=h\.invoice_id/s,
  );
  assert.match(
    generationCore,
    /nullif\(segment\.value->>'nhsp_shift_id',''\),\s*nullif\(segment\.value->>'shift_id',''\)\)=ns\.id::text/s,
  );
  assert.match(
    generationCore,
    /source_segments as materialized \([\s\S]*x\.value->>'nhsp_shift_id'[\s\S]*x\.value->>'shift_id'[\s\S]*source_imports as materialized/s,
  );
  assert.match(
    generationCore,
    /r\.external_row_key in\(\s*select jsonb_array_elements_text\(g\.reversal_row_keys\)\)[\s\S]*jsonb_build_object\('reversal_state','REVERSED'\)/s,
  );
});

test('generation revalidation correlates the canonical selection identity in every phase', () => {
  const generationCore = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/'
    + '27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
  );

  const canonicalSelectionJoins = generationCore.match(
    /when left\(coalesce\(ri\.payload_json->>'selection_key',''\),9\)='generate:'\s*then substr\(ri\.payload_json->>'selection_key',10\)\s*else ri\.payload_json->>'group_key' end/g,
  ) || [];
  assert.equal(canonicalSelectionJoins.length, 2);
  assert.doesNotMatch(
    generationCore,
    /where r\.group_key=ri\.payload_json->>'group_key'/,
  );
  const vatWeekFallbacks = generationCore.match(
    /ts_vat\.week_ending_date::text/g,
  ) || [];
  assert.equal(vatWeekFallbacks.length, 2);
});

test('generation establishes the invoice header before downstream ownership writes', () => {
  const generationCore = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/'
    + '27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
  );

  assert.match(
    generationCore,
    /target_headers as materialized \(\s*select e\.chunk_id,e\.invoice_id\s+from existing_target_headers e\s+where not exists\(\s*select 1\s+from inserted_headers h\s+where h\.id=e\.invoice_id\)\s*\)/s,
  );
  assert.match(
    generationCore,
    /deferred_new_headers as \([\s\S]*set status='QUEUED',\s*phase='COMMIT'[\s\S]*from inserted_headers h[\s\S]*join write_eligible_chunks vc on vc\.planned_invoice_id=h\.id/s,
  );
  assert.match(
    generationCore,
    /segment_lock_targets_pre as materialized \([\s\S]*where cardinality\(coalesce\(p\.segment_ids,array\[\]::text\[\]\)\)>0\s+and exists\(\s*select 1\s+from public\.invoices existing_header\s+where existing_header\.id=s\.planned_invoice_id\)/s,
  );
  assert.match(
    generationCore,
    /from valid_chunks vc\s+join target_headers h\s+on h\.chunk_id=vc\.id and h\.invoice_id=vc\.planned_invoice_id/s,
  );
  assert.match(
    generationCore,
    /whole_lock as \([\s\S]*join target_headers h\s+on h\.chunk_id=vc\.id and h\.invoice_id=vc\.planned_invoice_id/s,
  );
  assert.match(
    generationCore,
    /nhsp_shift_inclusion as \([\s\S]*from source_rows s\s+join target_headers h\s+on h\.chunk_id=s\.chunk_id and h\.invoice_id=s\.planned_invoice_id/s,
  );
  assert.match(
    generationCore,
    /hr_sources as \([\s\S]*from members m\s+join target_headers h on h\.chunk_id=m\.chunk_id/s,
  );
  assert.match(
    generationCore,
    /source_segments as materialized \([\s\S]*from source_rows s\s+join target_headers h\s+on h\.chunk_id=s\.chunk_id and h\.invoice_id=s\.planned_invoice_id/s,
  );
});

test('root repeatable installs every changed nested Invoice V8 authority', () => {
  const runtimeAuthority = read(
    'supabase/repeatable/28072026_1609_invoice_async_v8_runtime_authority.sql',
  );
  for (const filename of [
    '27072026_1042_08_private_invoice_batch_selection_rules_v2.sql',
    '27072026_1250_08a_private_invoice_batch_query_validate_v2.sql',
    '27072026_1806_private_invoice_batch_issue_classification_v2.sql',
    '27072026_1501_private_invoice_batch_generate_candidate_keys_v2.sql',
    '27072026_1501_private_invoice_batch_issue_candidate_keys_v2.sql',
    '23072026_2207_private_invoice_generation_resolve_command_groups.sql',
    '23072026_2207_private_invoice_correction_validate_batch.sql',
    '23072026_2207_private_invoice_operation_rollup_batch.sql',
    '27072026_1042_09_private_invoice_batch_generate_candidate_rows_v2.sql',
    '27072026_1042_10_private_invoice_batch_issue_candidate_rows_v2.sql',
    '27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
    '23072026_2207_private_invoice_issue_validate_batch.sql',
  ]) {
    assert.match(
      runtimeAuthority,
      new RegExp(filename.replaceAll('.', '\\.')),
    );
  }
});

test('committed TEST configuration keeps interactive enabled and scheduled disabled', () => {
  const wrangler = read('wrangler.toml');
  const functionHashes = JSON.parse(read(
    'supabase/contracts/27072026_1250_invoice_async_v8_function_hashes.json',
  ));
  const pipelineFlags = [
    ...wrangler.matchAll(/INVOICE_ASYNC_PIPELINE_ENABLED\s*=\s*"([^"]+)"/g),
  ];
  const scheduledFlags = [
    ...wrangler.matchAll(/INVOICE_ASYNC_SCHEDULED_ENABLED\s*=\s*"([^"]+)"/g),
  ];
  const manifestValues = [
    ...wrangler.matchAll(
      /INVOICE_ASYNC_EXPECTED_FUNCTION_MANIFEST\s*=\s*"([^"]+)"/g,
    ),
  ];
  const manifestEnforcementValues = [
    ...wrangler.matchAll(
      /INVOICE_ASYNC_FUNCTION_MANIFEST_ENFORCED\s*=\s*"([^"]+)"/g,
    ),
  ];
  const buildValues = [
    ...wrangler.matchAll(/INVOICE_ASYNC_BUILD_ID\s*=\s*"([^"]+)"/g),
  ];

  assert.ok(pipelineFlags.length > 0);
  assert.ok(scheduledFlags.length > 0);
  assert.ok(manifestValues.length >= 2);
  assert.equal(manifestEnforcementValues.length, 2);
  assert.ok(buildValues.length >= 2);
  assert.equal(pipelineFlags[0][1], 'false');
  assert.equal(pipelineFlags[1][1], 'true');
  assert.ok(pipelineFlags.slice(2).every(match => match[1] === 'false'));
  assert.ok(scheduledFlags.every(match => match[1] === 'false'));
  assert.equal(manifestValues[0][1], functionHashes.aggregate_sha256);
  assert.equal(manifestValues[1][1], functionHashes.aggregate_sha256);
  assert.equal(manifestEnforcementValues[0][1], 'true');
  assert.equal(manifestEnforcementValues[1][1], 'false');
  assert.equal(buildValues[1][1], 'invoice-async-v8-test-20260730-r47');
});

test('manual and QR timesheet document planning fails closed without an immutable source asset', () => {
  const documentAdvance = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql',
  );
  assert.match(
    documentAdvance,
    /missing_manual_timesheet_source as materialized \([\s\S]*'MANUAL_TIMESHEET_ASSET_REQUIRED'[\s\S]*upper\(coalesce\(it\.submission_mode,''\)\) in\('MANUAL','QR'\)[\s\S]*it\.manual_document_asset_id is null/s,
  );
  assert.match(
    documentAdvance,
    /l\.entity_type='INVOICE'\s*or exists\([\s\S]*upper\(coalesce\(it\.submission_mode,''\)\)='ELECTRONIC'/s,
  );
  assert.doesNotMatch(
    documentAdvance,
    /else 'ELECTRONIC_TIMESHEET' end input_type[\s\S]{0,500}or not exists\(/s,
  );
});

test('direct manual and QR timesheet documents use the exact registered asset', () => {
  const documentAdvance = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_private_invoice_document_advance_batch.sql',
  );
  assert.match(
    documentAdvance,
    /direct_timesheet as materialized \([\s\S]*join public\.timesheets t[\s\S]*left join public\.invoice_document_assets a\s+on a\.id=t\.manual_document_asset_id/s,
  );
  assert.match(
    documentAdvance,
    /blocked_direct_timesheet_source as materialized \([\s\S]*'MANUAL_TIMESHEET_ASSET_REQUIRED'[\s\S]*dt\.submission_mode in\('MANUAL','QR'\)[\s\S]*dt\.asset_id is null/s,
  );
  assert.match(
    documentAdvance,
    /select dt\.chunk_id,dt\.document_version_id,0::integer,\s*'ASSET',dt\.source_kind,dt\.source_id,dt\.source_revision,\s*dt\.asset_id[\s\S]*dt\.submission_mode in\('MANUAL','QR'\) and dt\.asset_id is not null/s,
  );
  assert.match(
    documentAdvance,
    /from linked l\s+left join direct_timesheet dt on dt\.chunk_id=l\.id\s+where l\.entity_type='INVOICE' or dt\.submission_mode='ELECTRONIC'\s+on conflict/s,
  );
  assert.match(
    documentAdvance,
    /status=case when exists\(\s*select 1 from direct_timesheet dt\s*where dt\.chunk_id=l\.id\s*and dt\.submission_mode in\('MANUAL','QR'\)\s*and dt\.asset_status='READY'\)\s*then 'QUEUED' else 'WAITING' end/s,
  );
});

test('single-part normalised assets use the direct-key READY representation', () => {
  const workComplete = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_invoice_work_complete_batch.sql',
  );
  assert.match(
    workComplete,
    /normalised_manifest_json=case when jsonb_array_length\(r\.manifest\)=1\s*then '\[\]'::jsonb else r\.manifest end/s,
  );
  assert.match(
    workComplete,
    /normalised_r2_key=case when jsonb_array_length\(r\.manifest\)=1\s*then r\.manifest->0->>'r2_key' else null end/s,
  );
  assert.match(
    workComplete,
    /normalised_manifest_hash=case when jsonb_array_length\(r\.manifest\)>1\s*then r\.manifest_hash else null end/s,
  );
});

test('repeatable deployment avoids no-op workbench session and job table locks', () => {
  const bankingTables = read('supabase/repeatable/08042026_1151_newtablesbanking.sql');
  assert.match(
    bankingTables,
    /IF \(\s*SELECT count\(\*\)[\s\S]*banking_pay_workbench_sessions[\s\S]*\) <> 14 THEN\s*ALTER TABLE public\.banking_pay_workbench_sessions/s,
  );
  assert.match(
    bankingTables,
    /IF EXISTS \(\s*SELECT 1\s*FROM public\.banking_pay_workbench_sessions[\s\S]*\) THEN\s*UPDATE public\.banking_pay_workbench_sessions/s,
  );
  assert.match(
    bankingTables,
    /column_default IS DISTINCT FROM 'gen_random_uuid\(\)'[\s\S]*THEN\s*ALTER TABLE public\.banking_pay_workbench_sessions/s,
  );
  assert.match(
    bankingTables,
    /IF \(\s*SELECT count\(\*\)[\s\S]*banking_pay_workbench_jobs[\s\S]*\) <> 18 THEN\s*ALTER TABLE public\.banking_pay_workbench_jobs/s,
  );
  assert.match(
    bankingTables,
    /IF EXISTS \(\s*SELECT 1\s*FROM public\.banking_pay_workbench_jobs[\s\S]*\) THEN\s*UPDATE public\.banking_pay_workbench_jobs/s,
  );
  assert.match(
    bankingTables,
    /column_name = 'status' AND column_default IS DISTINCT FROM '''QUEUED''::text'[\s\S]*THEN\s*ALTER TABLE public\.banking_pay_workbench_jobs/s,
  );
});

test('contract override defaults work without a client settings row', () => {
  const extras = read('supabase/repeatable/19012026_extras.sql');
  const triggerFunction = extras.match(
    /create or replace function public\.contracts_enforce_overrideclientsettings\(\)[\s\S]*?\n\$\$;/i,
  )?.[0] || '';
  assert.ok(triggerFunction);
  assert.doesNotMatch(triggerFunction, /\bcs record\b/i);
  assert.match(triggerFunction, /v_no_timesheet_required boolean;/i);
  assert.match(
    triggerFunction,
    /coalesce\(new\.no_timesheet_required,\s*v_no_timesheet_required,\s*false\)/i,
  );
  assert.match(
    triggerFunction,
    /new\.manual_invoices_alt_email_address := v_manual_invoices_alt_email_address;/i,
  );
});

test('contract override controls HealthRoster invoice validation', () => {
  const timesheetSummary = read('supabase/repeatable/02052026_1528_fast_timesheet_reading.sql');
  const canonicalSummaryViews = read(
    'supabase/repeatable/19122025_add_ready_to_pay_to_timesheets_summary_views.sql',
  );
  const effectiveHrValidation = (
    timesheetSummary.match(
      /CASE WHEN ct0\.overrideclientsettings THEN ct0\.requires_hr ELSE NULL::boolean END,\s*ch0\.hr_validation_required,\s*FALSE\s*\)\s+AS client_hr_validation_required/gi,
    ) || []
  );
  assert.equal(effectiveHrValidation.length, 2);
  assert.doesNotMatch(
    timesheetSummary,
    /COALESCE\(ch0\.hr_validation_required,\s*FALSE\)\s+AS client_hr_validation_required/i,
  );
  const canonicalEffectiveHrValidation = (
    canonicalSummaryViews.match(
      /CASE WHEN ct\.overrideclientsettings THEN ct\.requires_hr END,\s*ch\.hr_validation_required,\s*false\s*\)\s+AS client_hr_validation_required/gi,
    ) || []
  );
  assert.equal(canonicalEffectiveHrValidation.length, 4);
  assert.doesNotMatch(
    canonicalSummaryViews,
    /COALESCE\(ch\.hr_validation_required,\s*false\)\s+AS client_hr_validation_required/i,
  );
});

test('normalised timesheet evidence adopts the exact registered asset revision', () => {
  const workComplete = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_invoice_work_complete_batch.sql',
  );
  assert.match(
    workComplete,
    /update public\.timesheet_evidence e\s+set document_asset_id=a\.id,\s*source_revision=a\.source_revision,\s*processing_state='READY',\s*processing_error_json=null\s+from updated_assets a/s,
  );
});
