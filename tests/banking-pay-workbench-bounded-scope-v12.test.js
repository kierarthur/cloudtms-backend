import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = relativePath => readFileSync(new URL(relativePath, repoRoot), 'utf8');

const migrationPath = 'supabase/migrations/04082026_1134_banking_pay_bounded_scope_v12.sql';
const migration = read(migrationPath);
const closure = read(
  'supabase/repeatable/04082026_1151_pay_workbench_timesheet_dependency_closure_v2.sql'
).replace(/\r\n/g, '\n');
const selector = read('supabase/repeatable/04082026_1144_pay_workbench_candidate_bounded_scope_v1.sql');
const dispatcher = read('supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql');
const syncCore = read('supabase/repeatable/04082026_1210_pay_sync_overpayments_from_workbench_workspace_v1.sql');
const triggers = read('supabase/repeatable/04082026_1234_banking_pay_bounded_scope_triggers_v12.sql');
const markCandidateDirty = read('supabase/repeatable/04082026_1219_pay_workbench_mark_candidate_dirty.sql');
const markFinanceDirty = read('supabase/repeatable/04082026_1219_pay_workbench_mark_finance_case_dirty.sql');

const newFunctionFiles = [
  '04082026_1139_pay_workbench_scope_invalidate_v1.sql',
  '04082026_1141_pay_workbench_source_build_attempt_claim_start_v1.sql',
  '04082026_1143_pay_workbench_source_build_attempt_execute_v1.sql',
  '04082026_1144_pay_workbench_candidate_bounded_scope_v1.sql',
  '04082026_1146_pay_workbench_timesheet_input_fingerprint_v1.sql',
  '04082026_1147_pay_current_timesheet_entitlement_components_from_build_v1.sql',
  '04082026_1151_pay_workbench_timesheet_dependency_closure_v2.sql',
  '04082026_1202_pay_workbench_financial_scope_dirty_transition_v1.sql',
  '04082026_1210_pay_sync_overpayments_from_workbench_workspace_v1.sql',
];

const changedFunctionFiles = [
  '04082026_1219_pay_workbench_dirty_event_enqueue.sql',
  '04082026_1219_pay_workbench_enqueue_candidate_refresh.sql',
  '04082026_1219_pay_workbench_scope_change_finalize_trg_v1.sql',
  '04082026_1219_candidate_pay_method_change_refresh_scope_v1.sql',
  '04082026_1219_pay_workbench_contract_client_dirty_fanout_chunk.sql',
  '04082026_1213_pay_workbench_candidate_source_build_chunk.sql',
  '04082026_1210_pay_sync_overpayments_from_preview.sql',
  '04082026_1219_pay_workbench_enqueue_stage_continuation.sql',
  '04082026_1219_pay_workbench_complete_job.sql',
  '04082026_1219_pay_workbench_fail_job.sql',
  '04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql',
  '04082026_1219_pay_workbench_worker_drain_chunk.sql',
  '04082026_1219_pay_workbench_mark_candidate_dirty.sql',
  '04082026_1219_pay_workbench_mark_finance_case_dirty.sql',
  '04082026_1219_pay_timesheet_summary_pay_state_refresh_trigger.sql',
  '04082026_1219_candidate_delete_eligibility.sql',
  '04082026_1219_candidate_delete_apply.sql',
];

const sourceJobCompatibilityFiles = [
  '04082026_1302_pay_workbench_repair_invalid_source_build_poison.sql',
  '04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql',
  '04082026_1302_pay_workbench_session_replay_replaced_queue_v1.sql',
];

function stableIds(source, kind) {
  return [...source.matchAll(new RegExp(`DB-${kind}-(\\d{3})`, 'g'))]
    .map(match => Number(match[1]));
}

test('V1.2.4 SQL artifacts use timestamped repository placement and one identity per function', () => {
  const migrationNames = readdirSync(new URL('supabase/migrations/', repoRoot))
    .filter(name => name.includes('banking_pay_bounded_scope_v12'));
  assert.deepEqual(migrationNames, ['04082026_1134_banking_pay_bounded_scope_v12.sql']);

  const repeatableNames = new Set(readdirSync(new URL('supabase/repeatable/', repoRoot)));
  for (const name of [...newFunctionFiles, ...changedFunctionFiles, ...sourceJobCompatibilityFiles, '04082026_1234_banking_pay_bounded_scope_triggers_v12.sql']) {
    assert.match(name, /^\d{8}_\d{4}_[a-z0-9_]+\.sql$/);
    assert.ok(repeatableNames.has(name), `${name} must be in supabase/repeatable`);
  }

  for (const name of [...newFunctionFiles, ...changedFunctionFiles, ...sourceJobCompatibilityFiles]) {
    const source = read(`supabase/repeatable/${name}`);
    assert.equal((source.match(/CREATE OR REPLACE FUNCTION/gi) || []).length, 1, `${name} must replace exactly one function identity`);
  }
});

test('schema surface is exactly eight private tables, 88 constraints and 47 indexes', () => {
  const tableNames = [...migration.matchAll(/CREATE TABLE private\.([a-z0-9_]+)/gi)].map(match => match[1]);
  assert.deepEqual(tableNames, [
    'banking_pay_workbench_candidate_scope_registry',
    'banking_pay_workbench_timesheet_scope_state',
    'banking_pay_workbench_economic_builds',
    'banking_pay_workbench_economic_build_scope',
    'banking_pay_workbench_economic_build_facts',
    'banking_pay_workbench_stage_attempts',
    'banking_pay_workbench_economic_build_fact_pages',
    'banking_pay_workbench_canonical_stage_lines',
  ]);

  const constraintIds = stableIds(migration, 'CONSTRAINT');
  const expectedConstraints = Array.from({ length: 89 }, (_, index) => index + 1).filter(id => id !== 22);
  assert.deepEqual([...new Set(constraintIds)].sort((a, b) => a - b), expectedConstraints);

  const explicitIndexes = (migration.match(/^CREATE\s+(?:UNIQUE\s+)?INDEX\s+/gim) || []).length;
  const primaryIndexes = (migration.match(/\bPRIMARY KEY\s*\(/gi) || []).length;
  const uniqueConstraintIndexes = (migration.match(/\bUNIQUE(?:\s+NULLS\s+NOT\s+DISTINCT)?\s*\(/gi) || []).length;
  assert.equal(explicitIndexes + primaryIndexes + uniqueConstraintIndexes, 47);
  assert.equal((migration.match(/^\s*CONSTRAINT\s+/gim) || []).length + (migration.match(/ADD CONSTRAINT/gi) || []).length, 88);
  assert.match(migration, /DB-INDEX-002 through 007/i);
  assert.match(migration, /DB-INDEX-038 through 044/i);
  assert.match(migration, /DB-INDEX-052 through 054/i);

  assert.deepEqual(
    [...migration.matchAll(/ALTER TABLE public\.([a-z0-9_]+)/gi)].map(match => match[1]),
    ['banking_pay_workbench_jobs', 'banking_pay_workbench_jobs', 'settings_defaults', 'settings_defaults'],
  );
  assert.doesNotMatch(migration, /CREATE\s+(?:TYPE|VIEW)\b/i);
});

test('canonical staging identity is a PostgreSQL constraint with NULLS NOT DISTINCT semantics', () => {
  assert.match(
    migration,
    /CONSTRAINT bpay_wb_canonical_stage_identity_uq\s+UNIQUE NULLS NOT DISTINCT \(build_id,timesheet_id,line_key\)/i,
  );
  assert.match(migration, /DB-CONSTRAINT-079 \/ DB-INDEX-035:[^\n]*null-aware identity/i);
  assert.doesNotMatch(migration, /COALESCE\(timesheet_id[\s\S]*00000000-0000-0000-0000-000000000000/i);
});

test('private durable authority is not exposed to browser or service roles', () => {
  for (const table of [
    'candidate_scope_registry', 'timesheet_scope_state', 'economic_builds', 'economic_build_scope',
    'economic_build_facts', 'economic_build_fact_pages', 'stage_attempts', 'canonical_stage_lines',
  ]) {
    assert.match(
      migration,
      new RegExp(`REVOKE ALL ON TABLE private\\.banking_pay_workbench_${table} FROM PUBLIC, anon, authenticated, service_role`, 'i'),
    );
  }
  assert.doesNotMatch(migration, /GRANT\s+(?:SELECT|INSERT|UPDATE|DELETE|ALL)[\s\S]*TO\s+(?:anon|authenticated|service_role)/i);
});

test('ordinary scope begins from active state and does not rediscover lifetime history', () => {
  assert.match(selector, /banking_pay_workbench_timesheet_scope_state[\s\S]*economic_state IN \('DIRTY','LIVE'\)/i);
  assert.match(selector, /status='CURRENT'/i);
  assert.match(selector, /cleared_at_utc IS NULL[\s\S]*written_off_at_utc IS NULL/i);
  assert.doesNotMatch(selector, /pay_payment_correction_items/i);
  assert.doesNotMatch(selector, /FROM public\.pay_batch_items\s+AS/i);
});

test('dependency closure is uncapped and final sealing is bounded and metadata-only', () => {
  assert.match(closure, /v_limit integer:=LEAST\(GREATEST\(COALESCE\(p_limit,25\),2\),25\)/i);
  assert.doesNotMatch(closure, /p_max_members|LIMIT\s+100\b|LEAST\([^\n]*100\)/i);
  for (const phase of ['ANCHOR_INITIALISE', 'ANCHOR_PROPAGATE', 'EDGE_UNIT_TAG', 'UNIT_DIGEST', 'ROW_SEAL', 'COMPLETE']) {
    assert.match(closure, new RegExp(`'${phase}'`));
  }

  const completeStart = closure.indexOf("ELSIF v_phase='COMPLETE' THEN");
  const completeEnd = closure.indexOf('\n  ELSE\n', completeStart);
  assert.ok(completeStart >= 0 && completeEnd > completeStart);
  const complete = closure.slice(completeStart, completeEnd);
  assert.match(complete, /tagged_edge_count<>v_build\.dependency_edge_count/i);
  assert.match(complete, /row_seal_count<>v_build\.scope_count/i);
  assert.match(complete, /closure_status<>'SEALED'[\s\S]*LIMIT 1/i);
  assert.doesNotMatch(complete, /count\s*\(|string_agg\s*\(|array_agg\s*\(|GROUP BY|\bJOIN\b/i);
});

test('fact collection and legacy bootstrap page independently of total candidate size', () => {
  assert.match(dispatcher, /v_fact_limit integer:=LEAST\(GREATEST\(COALESCE\(p_limit,25\),1\),25\)/i);
  assert.match(dispatcher, /LIMIT v_fact_limit\+1/g);
  assert.match(dispatcher, /cursor_kind','WORKSPACE_FACT'/i);
  assert.match(dispatcher, /is_family_final/i);
  assert.match(dispatcher, /BOOTSTRAP_DISCOVERY/i);
  assert.match(dispatcher, /LIMIT 251/g);
  assert.match(dispatcher, /LIMIT 250/g);
  assert.match(dispatcher, /FROM public\.timesheets timesheet_row\s+JOIN public\.contracts contract_row/i);
  assert.match(dispatcher, /v_bootstrap_next_stream:='SCOPE_CLOSURE'/i);
  assert.match(closure, /bootstrap_stream','CLASSIFY_UNITS'/i);
  assert.match(dispatcher, /classification_phase','EVIDENCE'/i);
  assert.match(dispatcher, /classification_phase','APPLY'/i);
  assert.match(dispatcher, /bootstrap_stream','RESET_FACTS'/i);
  assert.match(dispatcher, /v_bootstrap_stream='RESET_SCOPE'|THEN 'RESET_SCOPE'/i);
  assert.doesNotMatch(dispatcher, /SELECT count\(\*\)[^;]*economic_state='DIRTY'/is);
  assert.doesNotMatch(dispatcher, /p_max_members|LIMIT\s+100\b/i);
});

test('every installed source-build enqueue writer creates the one permitted typed null-build job', () => {
  const sources = [
    read('supabase/repeatable/04082026_1219_pay_workbench_enqueue_candidate_refresh.sql'),
    read('supabase/repeatable/04082026_1219_pay_workbench_contract_client_dirty_fanout_chunk.sql'),
    ...sourceJobCompatibilityFiles.map(name => read(`supabase/repeatable/${name}`)),
  ];

  for (const source of sources) {
    assert.match(source, /economic_build_id/i);
    assert.match(source, /private_stage/i);
    assert.match(source, /private_cursor_kind/i);
    assert.match(source, /private_cursor_json/i);
    assert.match(source, /private_stage_version/i);
    assert.match(source, /BUILD_INITIALISE/i);
  }
});

test('candidate-wide reconciliation stages privately and publication switches CURRENT authority atomically', () => {
  assert.match(syncCore, /INSERT INTO private\.banking_pay_workbench_canonical_stage_lines/i);
  assert.doesNotMatch(syncCore, /INSERT INTO public\.banking_pay_workbench_candidate_source_lines/i);

  const publishStart = dispatcher.indexOf("IF v_stage='SOURCE_PUBLISH' THEN");
  const publishEnd = dispatcher.indexOf("IF v_stage='BUILD_CLEANUP' THEN", publishStart);
  assert.ok(publishStart >= 0 && publishEnd > publishStart);
  const publish = dispatcher.slice(publishStart, publishEnd);
  assert.match(publish, /stage_status<>'VERIFIED'/i);
  assert.match(publish, /status='SUPERSEDED'/i);
  assert.match(publish, /INSERT INTO public\.banking_pay_workbench_candidate_source_lines/i);
  assert.match(publish, /'CURRENT'/i);
  assert.match(publish, /CANONICAL_PUBLICATION_DIGEST_MISMATCH/i);
  assert.doesNotMatch(dispatcher, /^\s*(?:COMMIT|ROLLBACK)\s*;/im);
});

test('all 18 invalidation backstops and nine independent finance observers use one statement adapter', () => {
  assert.equal((triggers.match(/FOR EACH STATEMENT/gi) || []).length, 27);
  assert.equal((triggers.match(/EXECUTE FUNCTION private\.pay_workbench_financial_scope_dirty_transition_v1\(\)/gi) || []).length, 27);
  assert.equal((triggers.match(/trg_bpay_wb_observe_(?:advances|components|events)_(?:insert|update|delete)/gi) || []).length, 18);
  for (const parent of ['pay_batch_candidates', 'pay_batches', 'pay_bank_transfers']) {
    assert.match(triggers, new RegExp(`ON public\\.${parent}[\\s\\S]{0,180}OLD TABLE AS old_rows`, 'i'));
  }
});

test('retained finance dirty triggers declare exact effects before finance DML', () => {
  for (const name of [
    'trg_pay_workbench_mark_candidate_dirty__pay_advances',
    'trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_com',
    'trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_eve',
  ]) {
    assert.match(triggers, new RegExp(`CREATE TRIGGER ${name}[\\s\\S]{0,180}BEFORE INSERT OR DELETE OR UPDATE`, 'i'));
  }
  for (const source of [markCandidateDirty, markFinanceDirty]) {
    assert.match(source, /TG_WHEN<>'BEFORE'/i);
    assert.match(source, /INSERT INTO pg_temp\._bpay_wb_expected_effects/i);
    assert.match(source, /v_internal_before_digest/i);
    assert.match(source, /v_internal_after_digest/i);
    assert.match(source, /ARRAY\['created_at','created_at_utc','updated_at','updated_at_utc'/i);
    assert.match(source, /SET[\s\S]{0,160}proposed=true/i);
    assert.doesNotMatch(source, /SET[\s\S]{0,120}observed=true/i);
    assert.match(source, /PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH/i);
  }
});
