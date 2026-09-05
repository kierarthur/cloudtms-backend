const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const migrationPath = path.join(root, 'supabase', 'migrations', '02092026_2310_banking_pay_draft_frozen_certificate_v8.sql');
const adaptersPath = path.join(root, 'supabase', 'repeatable', '02092026_2311_banking_pay_draft_frozen_certificate_adapters_v8.sql');
const ownerPath = path.join(root, 'supabase', 'repeatable', '02092026_2312_banking_pay_draft_row_backed_orchestration_v8.sql');
const migration = fs.readFileSync(migrationPath, 'utf8');
const adapters = fs.readFileSync(adaptersPath, 'utf8');
const owner = fs.readFileSync(ownerPath, 'utf8');
const combined = `${migration}\n${adapters}\n${owner}`;

const exactTables = [
  'banking_pay_draft_frozen_certificate_scopes_v8',
  'banking_pay_draft_frozen_constituent_refs_v8',
  'banking_pay_draft_frozen_constituent_payloads_v8',
  'banking_pay_draft_frozen_partition_refs_v8',
  'banking_pay_draft_frozen_candidate_inputs_v8',
  'banking_pay_draft_frozen_candidate_scopes_v8',
  'banking_pay_draft_frozen_stage_receipts_v8',
  'banking_pay_draft_operation_created_batches_v8',
  'banking_pay_draft_operation_terminal_results_v8',
  'banking_pay_draft_operation_provenance_events_v8',
  'banking_pay_draft_frozen_candidate_scope_members_v8',
  'banking_pay_draft_phase_units_v1',
  'banking_pay_draft_owner_receipts_v1',
  'banking_pay_draft_finalizer_iterations_v8',
  'banking_pay_draft_constituent_parity_results_v8'
];

const exactIndexes = [
  'banking_pay_draft_frozen_refs_v8_page_idx',
  'banking_pay_draft_frozen_payloads_v8_scope_page_idx',
  'banking_pay_draft_frozen_payloads_v8_timesheet_page_idx',
  'banking_pay_draft_frozen_partitions_v8_page_idx',
  'banking_pay_draft_frozen_candidate_inputs_v8_page_idx',
  'banking_pay_draft_frozen_scopes_v8_page_idx',
  'banking_pay_draft_created_batches_v8_page_idx',
  'banking_pay_operations_v8_legacy_activation_idx',
  'banking_pay_draft_scope_members_v8_page_idx',
  'banking_pay_draft_scope_members_v8_constituent_idx',
  'banking_pay_draft_finalizer_iterations_v8_next_idx',
  'banking_pay_draft_parity_results_v8_page_idx'
  ,'banking_pay_draft_phase_units_v1_next_idx'
  ,'banking_pay_draft_owner_receipts_v1_page_idx'
];

test('storage matches the exact V8 H2 relation and index inventory', () => {
  assert.equal((migration.match(/CREATE TABLE IF NOT EXISTS private\./g) || []).length, 15);
  assert.equal((migration.match(/CREATE INDEX IF NOT EXISTS/g) || []).length, 14);
  for (const table of exactTables) {
    assert.match(migration, new RegExp(`CREATE TABLE IF NOT EXISTS private\\.${table}\\s*\\(`));
  }
  for (const index of exactIndexes) {
    assert.match(migration, new RegExp(`CREATE INDEX IF NOT EXISTS ${index}\\b`));
  }
  assert.doesNotMatch(migration, /banking_pay_draft_phase_manifests_v1/i);
  assert.match(migration, /banking_pay_draft_phase_units_v1/i);
  assert.match(migration, /banking_pay_draft_owner_receipts_v1/i);
});

test('normalized storage contains no complete selected-set or Candidate-scope array', () => {
  assert.doesNotMatch(migration, /selected_preview_row_ids_json/i);
  assert.doesNotMatch(migration, /selected_canonical_preview_lines_json/i);
  assert.doesNotMatch(migration, /candidate_scope_ids_json/i);
  assert.match(migration, /constituent_count BETWEEN 1 AND 50000/);
  assert.match(migration, /member_ordinal BETWEEN 0 AND 49999/);
});

test('certificate staging uses bounded keyset pages and an explicit sentinel', () => {
  for (const functionName of [
    'pay_workbench_draft_certificate_constituent_ref_page_v8',
    'pay_workbench_draft_certificate_partition_ref_page_v8'
  ]) {
    assert.match(adapters, new RegExp(`CREATE OR REPLACE FUNCTION public\\.${functionName}`));
  }
  assert.match(adapters, /IF v_limit < 1 OR v_limit > 256/);
  assert.match(adapters, /LIMIT \(v_limit \+ 1\)/);
  assert.match(adapters, /v_has_more := v_candidate_count > v_limit/);
  assert.match(adapters, /v_canonical_bytes > 524288/);
  assert.match(adapters, /WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT/);
  assert.match(adapters, /terminal_sentinel_present/);
});

test('reference validation uses one session fence and zero Candidate locks', () => {
  assert.match(owner, /CREATE OR REPLACE FUNCTION public\.pay_workbench_settled_certificate_reference_validate_v8/);
  assert.match(owner, /FROM public\.banking_pay_workbench_sessions AS session_row[\s\S]*?FOR UPDATE/);
  assert.match(owner, /'candidate_locks_taken', 0/);
  assert.doesNotMatch(owner, /FROM public\.candidates[^;]*FOR (?:UPDATE|SHARE)/is);
  assert.doesNotMatch(owner, /FROM public\.banking_pay_workbench_candidate_publications[^;]*FOR (?:UPDATE|SHARE)/is);
  assert.match(owner, /authority_fence_generation IS DISTINCT FROM v_certificate\.authority_fence_generation/);
});

test('phase-unit staging pages the full normalized scope instead of accepting one global array', () => {
  assert.match(owner, /CREATE OR REPLACE FUNCTION public\.banking_pay_draft_phase_units_seed_v8/);
  assert.match(owner, /IF v_limit < 1 OR v_limit > 256/);
  assert.match(owner, /LIMIT \(v_limit \+ 1\)/);
  assert.match(owner, /candidate_scope_ordinal > COALESCE\(p_after_candidate_scope_ordinal, -1\)/);
  assert.match(owner, /INSERT INTO private\.banking_pay_draft_phase_units_v1/);
  assert.match(owner, /ON CONFLICT \(operation_id, phase, candidate_scope_ordinal\) DO NOTHING/);
  assert.match(owner, /v_total_unit_count <> v_scope\.partition_count/);
  assert.doesNotMatch(owner, /p_(?:units_json|candidate_scope_ids_json)/i);
  assert.doesNotMatch(owner, /INSERT INTO public\.banking_pay_operation_scope_units/i);
});

test('page-size tuning cannot change the total limit or financial-owner page size', () => {
  assert.equal((adapters.match(/p_limit integer DEFAULT 256/g) || []).length, 2);
  assert.equal((adapters.match(/v_limit integer := COALESCE\(p_limit, 256\)/g) || []).length, 2);
  assert.match(owner, /p_limit integer DEFAULT 256/);
  assert.match(owner, /v_limit integer := COALESCE\(p_limit, 256\)/);
  assert.match(migration, /constituent_count BETWEEN 1 AND 50000/);
  assert.match(migration, /requested_owner_page_size = 100/);
  assert.match(owner, /'financial_owner_chunk_limit', 100/);
  assert.doesNotMatch(combined, /50001[^\n]*(?:accept|allow)/i);
});

test('existing budgets are applied and no timeout is changed', () => {
  assert.match(owner, /banking_pay_hot_path_budget_apply\('WORKBENCH_CHUNK'\)/);
  assert.doesNotMatch(combined, /SET\s+(?:LOCAL\s+)?statement_timeout/i);
  assert.doesNotMatch(combined, /SET\s+(?:LOCAL\s+)?lock_timeout/i);
});

test('PostgreSQL conditional constructs are never schema-qualified', () => {
  assert.doesNotMatch(combined, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('all public V8 entry points are service-only', () => {
  for (const signature of [
    'pay_workbench_draft_certificate_constituent_ref_page_v8\\(uuid,integer,integer,text\\)',
    'pay_workbench_draft_certificate_partition_ref_page_v8\\(uuid,integer,integer,text\\)',
    'pay_workbench_draft_certificate_final_freeze_v8\\(uuid,text\\)',
    'pay_workbench_settled_certificate_reference_validate_v8\\(uuid,text,text,text\\)',
    'banking_pay_draft_phase_units_seed_v8\\(uuid,integer,integer,text\\)'
  ]) {
    assert.match(combined, new RegExp(`REVOKE ALL ON FUNCTION public\\.${signature} FROM PUBLIC, anon, authenticated`));
    assert.match(combined, new RegExp(`GRANT EXECUTE ON FUNCTION public\\.${signature} TO service_role`));
  }
});
