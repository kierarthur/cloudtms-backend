const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sourcePath = 'supabase/repeatable/03092026_1620_banking_pay_draft_integrity_row_backed_v8.sql';
const generatedPath = 'supabase/repeatable/04092026_1530_banking_pay_draft_integrity_advance_lookup_v8.sql';
const setwiseIntegrityPath = 'supabase/repeatable/05092026_0310_banking_pay_draft_integrity_setwise_v8.sql';
const setwiseIntegrityVerificationPath = 'supabase/verification/05092026_0311_banking_pay_draft_integrity_setwise_v8_verification.sql';
const setwiseIntegrityRuntimePath = 'tests/05092026_0312_banking_pay_draft_integrity_setwise_v8_runtime_verification.sql';
const currentReleasePath = 'supabase/release/current-release.json';
const indexPath = 'supabase/migrations/04092026_1520_banking_pay_draft_advanced_override_lookup_v8.sql';
const insertSourcePath = 'supabase/repeatable/02092026_1040_banking_pay_draft_insert_items_finance_handoff_v1.sql';
const insertGeneratedPath = 'supabase/repeatable/04092026_1340_banking_pay_draft_carry_forward_policy_transport_v8.sql';
const finalizerPath = 'supabase/repeatable/04092026_1400_banking_pay_draft_finalizer_summary_paging_v8.sql';

const advancedPredicateFragments = [
  "LOWER(COALESCE(payload_json->>'is_advanced', 'false')) IN ('true', 't', '1', 'yes', 'y')",
  "NULLIF(BTRIM(COALESCE(payload_json->>'advanced_override_id', '')), '') IS NOT NULL"
];

function publicIntegrityOwner(sql) {
  const marker = 'CREATE OR REPLACE FUNCTION public.pay_batch_assert_integrity';
  const start = sql.indexOf(marker);
  assert.notEqual(start, -1, 'public integrity owner missing');
  const end = sql.indexOf('$function$;', start);
  assert.notEqual(end, -1, 'public integrity owner terminator missing');
  return sql.slice(start, end + '$function$;'.length);
}

function validateAdvanceArtifacts(migration, generated) {
  assert.match(migration, /CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_payloads_v8_advanced_override_idx/);
  assert.match(migration, /operation_id,[\s\S]*candidate_id,[\s\S]*resolved_pay_channel,[\s\S]*constituent_ordinal/);
  for (const fragment of advancedPredicateFragments) assert.ok(migration.includes(fragment), fragment);
  assert.doesNotMatch(migration, /statement_timeout|lock_timeout/i);

  assert.match(generated, /CREATE OR REPLACE FUNCTION private\.pay_workbench_operation_selected_advanced_lines_v8\(/);
  assert.match(generated, /JOIN private\.banking_pay_draft_frozen_candidate_scope_members_v8 AS member/);
  assert.match(generated, /member\.candidate_scope_ordinal = frozen_scope\.candidate_scope_ordinal/);
  assert.match(generated, /member\.constituent_ordinal = payload\.constituent_ordinal/);
  assert.equal((generated.match(/CREATE OR REPLACE FUNCTION /g) || []).length, 2);
  assert.match(generated, /REVOKE ALL ON FUNCTION private\.pay_workbench_operation_selected_advanced_lines_v8\(uuid,uuid\)[\s\S]*FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(generated, /GRANT EXECUTE ON FUNCTION public\.pay_batch_assert_integrity\(uuid,uuid,uuid\)[\s\S]*TO service_role/);
}

function validateFinanceToSummaryContinuation(finalizer) {
  assert.match(finalizer, /v_summary_continuation boolean := false/);
  assert.match(finalizer, /reservation_pending_before_count'\)::integer > 0[\s\S]*summary_timesheets_refreshed'\)::integer = 0[\s\S]*summary_next_after_timesheet_id',''\) IS NULL/);
  assert.match(finalizer, /v_summary_continuation := true/);
  assert.match(finalizer, /IF v_bounded_v8 AND v_summary_continuation THEN/);
  assert.match(finalizer, /COUNT\(\*\) FILTER \(\s+WHERE v_summary_after_timesheet_id IS NULL\s+OR summary_source\.timesheet_id > v_summary_after_timesheet_id/);
  assert.match(finalizer, /AND \(\s+v_summary_after_timesheet_id IS NULL\s+OR batch_item\.timesheet_id > v_summary_after_timesheet_id\s+\)/);
  assert.match(finalizer, /v_summary_after_timesheet_id IS NOT NULL AND NOT COALESCE\(v_summary_cursor_found,false\)/);
  assert.doesNotMatch(finalizer, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout)/i);
}

function validateSetwiseIntegrityOwner(sql, priorSql) {
  const owner = publicIntegrityOwner(sql);
  const priorOwner = publicIntegrityOwner(priorSql);
  const checkCodes = value => [...value.matchAll(/SELECT '([^']+)'::text AS check_code/g)].map(match => match[1]);

  assert.equal((sql.match(/^CREATE OR REPLACE FUNCTION /gm) || []).length, 1);
  assert.deepEqual(checkCodes(owner), checkCodes(priorOwner), 'integrity check vocabulary/order changed');
  assert.equal(
    (owner.match(/private\.pay_workbench_operation_selected_lines_v8\(/g) || []).length,
    1,
    'the complete frozen selected set must be read once'
  );
  assert.match(owner, /WITH operation_scope_rows AS MATERIALIZED/);
  assert.match(owner, /operation_reservation_rows AS MATERIALIZED/);
  assert.match(owner, /operation_first_reservation_rows AS MATERIALIZED/);
  assert.match(owner, /operation_selected_source_lines AS MATERIALIZED/);
  assert.match(owner, /operation_selected_lines AS MATERIALIZED/);
  assert.match(owner, /operation_allocation_rows AS MATERIALIZED/);
  assert.match(owner, /operation_batch_items AS MATERIALIZED/);
  assert.match(owner, /allocation_row\.candidate_scope_id = selected_line\.candidate_scope_id/);
  assert.match(owner, /allocation_row\.bound_preview_row_id = selected_line\.preview_row_id/);
  assert.match(owner, /JOIN operation_batch_items AS item_row\s+ON item_row\.id = allocation_row\.pay_batch_item_id/);
  assert.match(owner, /upper\(btrim\(coalesce\(allocation_row\.status, ''\)\)\) IN \('ITEM_CREATED', 'ITEM_INSERTED'\)/);
  assert.doesNotMatch(owner, /operation_source_key LIKE \('%' \|\| selected_line\.preview_row_id \|\| '%'\)/);
  assert.doesNotMatch(owner, /pay_workbench_operation_selected_line_count_v8/);
  assert.doesNotMatch(sql, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
  assert.match(sql, /REVOKE ALL ON FUNCTION public\.pay_batch_assert_integrity\(uuid,uuid,uuid\)[\s\S]*FROM PUBLIC, anon, authenticated/);
  assert.match(sql, /GRANT EXECUTE ON FUNCTION public\.pay_batch_assert_integrity\(uuid,uuid,uuid\)[\s\S]*TO service_role/);
}

test('latest INSERT_ITEMS owner is regenerated from its optimized source', () => {
  const result = spawnSync(
    process.execPath,
    ['scripts/generate-banking-pay-draft-carry-forward-policy-transport-v8.mjs', '--check'],
    { cwd: root, encoding: 'utf8' }
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const source = read(insertSourcePath);
  const generated = read(insertGeneratedPath);
  assert.match(source, /allocation_page_scopes AS MATERIALIZED/);
  assert.match(source, /FROM allocation_page_scopes AS page_scope[\s\S]*pay_workbench_draft_scope_line_rows_v8/);
  assert.match(generated, /allocation_page_scopes AS MATERIALIZED/);
  assert.match(generated, /FROM allocation_page_scopes AS page_scope[\s\S]*pay_workbench_draft_scope_line_rows_v8/);
  assert.doesNotMatch(
    generated,
    /FROM allocation_page_keys\s+JOIN public\.banking_pay_operation_candidate_scope AS scope_row[\s\S]{0,700}pay_workbench_draft_scope_line_rows_v8/
  );
});

test('advance-only integrity lookup is indexed, scoped and generated exactly', () => {
  const result = spawnSync(
    process.execPath,
    ['scripts/generate-banking-pay-draft-integrity-advance-lookup-v8.mjs', '--check'],
    { cwd: root, encoding: 'utf8' }
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const migration = read(indexPath);
  const generated = read(generatedPath);
  validateAdvanceArtifacts(migration, generated);

  const sourceOwner = publicIntegrityOwner(read(sourcePath));
  const expectedOwner = sourceOwner.replace(
    'private.pay_workbench_operation_selected_lines_v8(p_operation_id, scope_row.id) AS line_element(value)',
    'private.pay_workbench_operation_selected_advanced_lines_v8(p_operation_id, scope_row.id) AS line_element(value)'
  );
  assert.equal(publicIntegrityOwner(generated), expectedOwner);
  assert.equal(
    (publicIntegrityOwner(generated).match(/pay_workbench_operation_selected_advanced_lines_v8/g) || []).length,
    1
  );
  assert.equal(
    (publicIntegrityOwner(generated).match(/pay_workbench_operation_selected_lines_v8/g) || []).length,
    3,
    'only the advance assertion may use the filtered reader'
  );
});

test('final Draft integrity owner preserves every check while replacing repeated wildcard work with exact set-wise identity joins', () => {
  const result = spawnSync(
    process.execPath,
    ['scripts/generate-banking-pay-draft-integrity-setwise-v8.mjs', '--check'],
    { cwd: root, encoding: 'utf8' }
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  validateSetwiseIntegrityOwner(read(setwiseIntegrityPath), read(generatedPath));
});

test('set-wise integrity guards reject policy, completeness, identity and timeout mutations', () => {
  const sql = read(setwiseIntegrityPath);
  const priorSql = read(generatedPath);
  for (const [label, mutation] of [
    ['scope materialisation', sql.replace('WITH operation_scope_rows AS MATERIALIZED', 'WITH operation_scope_rows AS')],
    ['selected-line materialisation', sql.replace('operation_selected_lines AS MATERIALIZED', 'operation_selected_lines AS')],
    ['candidate-scope identity', sql.replace('allocation_row.candidate_scope_id = selected_line.candidate_scope_id', 'true')],
    ['preview-row identity', sql.replace('allocation_row.bound_preview_row_id = selected_line.preview_row_id', 'true')],
    ['allocation-item identity', sql.replace(
      'JOIN operation_batch_items AS item_row\n            ON item_row.id = allocation_row.pay_batch_item_id',
      'JOIN operation_batch_items AS item_row\n            ON true'
    )],
    ['missing-item check', sql.replace("'MISSING_SELECTED_PREVIEW_ROW_ITEM'::text", "'REMOVED_SELECTED_ITEM_CHECK'::text")],
    ['wildcard lookup', sql.replace(
      'FROM operation_selected_lines AS selected_line',
      "FROM operation_selected_lines AS selected_line /* operation_source_key LIKE ('%' || selected_line.preview_row_id || '%') */"
    )],
    ['timeout relaxation', sql.replace("SET search_path TO 'public'", "SET search_path TO 'public'\n SET statement_timeout TO '30s'")]
  ]) {
    assert.throws(() => validateSetwiseIntegrityOwner(mutation, priorSql), label);
  }
});

test('set-wise integrity verification is mandatory for NEW and UPGRADE release paths', () => {
  const verification = read(setwiseIntegrityVerificationPath);
  const currentRelease = JSON.parse(read(currentReleasePath));

  assert.equal(currentRelease.verificationFiles.filter(path => path === setwiseIntegrityVerificationPath).length, 1);
  assert.equal(currentRelease.newVerificationFiles.filter(path => path === setwiseIntegrityVerificationPath).length, 1);
  assert.match(verification, /aclexplode\([\s\S]*acl\.grantee = 0[\s\S]*acl\.privilege_type = 'EXECUTE'/);
  assert.match(verification, /pay_workbench_operation_selected_lines_v8/);
  assert.match(verification, /MISSING_SELECTED_PREVIEW_ROW_ITEM/);
  assert.match(verification, /PAY_CHANNEL_SCOPE_TOTAL_MISMATCH/);
  assert.doesNotMatch(verification, /has_function_privilege\('PUBLIC'/);
  assert.doesNotMatch(verification, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
});

test('set-wise integrity runtime proof preserves budgets and exercises exact typed failures under rollback', () => {
  const runtime = read(setwiseIntegrityRuntimePath);
  for (const checkCode of [
    'MISSING_SELECTED_PREVIEW_ROW_ITEM',
    'ALLOCATION_ITEM_AMOUNT_MISMATCH',
    'CANDIDATE_SCOPE_TOTAL_MISMATCH',
    'PAY_CHANNEL_SCOPE_TOTAL_MISMATCH'
  ]) assert.match(runtime, new RegExp(checkCode));
  assert.match(runtime, /SET LOCAL statement_timeout = '15s'/);
  assert.match(runtime, /SET LOCAL lock_timeout = '1500ms'/);
  assert.match(runtime, /SET LOCAL idle_in_transaction_session_timeout = '30s'/);
  assert.equal((runtime.match(/^BEGIN;$/gm) || []).length, 1);
  assert.equal((runtime.match(/^ROLLBACK;$/gm) || []).length, 1);
  assert.match(runtime, /external_payment_actions":0/);
  assert.doesNotMatch(
    runtime,
    /\b(?:banking_pay_execute|prepare_provider|submit_provider|generate_remittance)\s*\(/i,
    'runtime proof must not invoke external-action owners'
  );
});

test('performance guards fail if either advance selector or membership fence is weakened', () => {
  const migration = read(indexPath);
  const generated = read(generatedPath);
  for (const mutation of [
    [migration.replace(advancedPredicateFragments[0], 'false'), generated],
    [migration.replace(advancedPredicateFragments[1], 'false'), generated],
    [migration, generated.replace('member.candidate_scope_ordinal = frozen_scope.candidate_scope_ordinal', 'true')],
    [migration, generated.replace('member.constituent_ordinal = payload.constituent_ordinal', 'true')]
  ]) {
    assert.throws(() => validateAdvanceArtifacts(mutation[0], mutation[1]));
  }
});

test('finance-only page hands off to the first summary page without inventing a cursor', () => {
  const finalizer = read(finalizerPath);
  validateFinanceToSummaryContinuation(finalizer);
  for (const mutation of [
    finalizer.replace("(v_prior_owner_result->>'reservation_pending_before_count')::integer > 0", 'true'),
    finalizer.replace('IF v_bounded_v8 AND v_summary_continuation THEN', 'IF v_bounded_v8 THEN'),
    finalizer.replace(/v_summary_after_timesheet_id IS NULL\s+OR summary_source\.timesheet_id > v_summary_after_timesheet_id/, 'summary_source.timesheet_id > v_summary_after_timesheet_id'),
    finalizer.replace(/v_summary_after_timesheet_id IS NULL\s+OR batch_item\.timesheet_id > v_summary_after_timesheet_id/, 'batch_item.timesheet_id > v_summary_after_timesheet_id')
  ]) {
    assert.throws(() => validateFinanceToSummaryContinuation(mutation));
  }
});
