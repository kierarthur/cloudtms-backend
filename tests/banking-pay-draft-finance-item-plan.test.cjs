const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8').replace(/\r\n/g, '\n');

const planSql = read(
  'supabase',
  'repeatable',
  '11082026_1535_pay_workbench_draft_finance_item_plan_v1.sql'
);
const seedSql = read(
  'supabase',
  'repeatable',
  '08082026_0717_pay_workbench_prepare_draft_allocation_rows_seed_sort_order.sql'
);
const financeSql = read(
  'supabase',
  'repeatable',
  '21072026_1235_49_pay_batch_apply_finance_adjustments.sql'
);

test('the operation finance-item plan is deterministic, bounded and owner-only', () => {
  assert.match(planSql, /^CREATE OR REPLACE FUNCTION private\.pay_workbench_draft_finance_item_plan_v1\(/);
  assert.match(planSql, /v_scope_count = 0 OR v_scope_count > 100/);
  assert.match(planSql, /scoped_row\.operation_source_key AS effective_planned_item_key/);
  assert.match(planSql, /NULLIF\(pg_catalog\.btrim\(COALESCE\(scoped_row\.operation_source_key, ''\)\), ''\) IS NOT NULL/);
  assert.match(planSql, /scoped_row\.effective_item_type = 'OVERPAYMENT_RECOVERY'/);
  assert.match(planSql, /allocation_basis_json, '\{\}'::jsonb\) - 'draft_finance_item_plan'/);
  assert.doesNotMatch(planSql, /pg_catalog\.coalesce\s*\(/i);
  assert.match(planSql, /SECURITY DEFINER\s+SET search_path TO ''/);
  assert.match(planSql, /REVOKE ALL ON FUNCTION private\.pay_workbench_draft_finance_item_plan_v1\(uuid,jsonb\) FROM PUBLIC, anon, authenticated, service_role/);
});

test('the frozen finance-item identity excludes the batch shell assigned after seeding', () => {
  const keyStart = planSql.indexOf('scoped_row.operation_source_key AS effective_planned_item_key');
  const keyEnd = planSql.indexOf('AS effective_planned_item_key', keyStart);
  assert.ok(keyStart >= 0 && keyEnd >= keyStart, 'planned-item identity block must exist');
  assert.doesNotMatch(planSql.slice(keyStart, keyStart + 100), /pay_batch_id/);
  assert.match(planSql, /keyed_row\.pay_batch_id,/);
});

test('Draft allocation seeding freezes the canonical plan and rejects replay drift', () => {
  assert.match(seedSql, /private\.pay_workbench_draft_finance_item_plan_v1\(p_operation_id, v_plan_scope_ids\)/);
  assert.match(seedSql, /'draft_finance_item_plan'[\s\S]*'planned_item_key'[\s\S]*'planned_item_amount'[\s\S]*'plan_digest'/);
  assert.match(seedSql, /RAISE EXCEPTION 'DRAFT_FINANCE_ITEM_PLAN_DRIFT'/);
});

test('operation Drafts materialise recoveries from the frozen plan rather than recapping them', () => {
  assert.match(financeSql, /and p_operation_id is null/);
  assert.match(financeSql, /FROM private\.pay_workbench_draft_finance_item_plan_v1\([\s\S]*materialisation\.planned_item_key/);
  assert.match(financeSql, /ROUND\(materialisation\.planned_item_amount, 2\)::numeric\(12,2\)/);
  assert.match(financeSql, /materialisation\.planned_item_key\s+FROM materialisation/);
  assert.match(financeSql, /DRAFT_FINANCE_ITEM_PLAN_DRIFT/);
});

test('planned Draft finance rows require exact signed two-way identity parity', () => {
  assert.match(
    financeSql,
    /batch_item\.operation_source_key = allocation_row\.allocation_basis_json#>>'\{draft_finance_item_plan,planned_item_key\}'[\s\S]*ROUND\(COALESCE\(batch_item\.amount_ex_vat, 0\), 2\) = ROUND\(COALESCE\(allocation_row\.allocated_amount, 0\), 2\)/
  );
  assert.match(financeSql, /missing_key_count[\s\S]*unexpected_key_count[\s\S]*duplicate_key_count[\s\S]*identity_or_amount_mismatch_count/);
  assert.match(financeSql, /batch_item\.operation_source_key LIKE \(p_operation_id::text \|\| ':%'\)/);
  assert.match(financeSql, /'code', 'DRAFT_FINANCE_ITEM_PLAN_PARITY_FAILED'/);
  assert.match(financeSql, /'code', 'OPERATION_ALLOCATION_ROWS_NOT_LINKED'/);
});
