const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const authorityPath = path.join(
  root,
  'supabase',
  'repeatable',
  '08082026_0820_pay_batch_freshness_scope_seed_active_items.sql'
);
const sql = fs.readFileSync(authorityPath, 'utf8');

test('operation freshness seeds active frozen payment items only', () => {
  assert.match(
    sql,
    /WHERE\s+batch_candidate\.pay_batch_id\s*=\s*v_pay_batch_id\s+AND\s+COALESCE\(batch_item\.is_voided,\s*false\)\s*=\s*false\s+AND\s*\(/i,
    'items already voided before the operation starts must not enter its active freshness scope'
  );
});

test('post-seed voiding remains detectable by the validator', () => {
  const validatorSql = fs.readFileSync(
    path.join(root, 'supabase', 'repeatable', '26052026_2100HRS_NEW_FUNCTIONS.sql'),
    'utf8'
  );
  assert.match(
    validatorSql,
    /CASE\s+WHEN\s+resolved\.live_is_voided\s+THEN\s+jsonb_build_array\('PAY_BATCH_ITEM_VOIDED'\)/i,
    'an item that becomes voided after active-scope seeding must still fail freshness'
  );
});

test('freshness authority preserves the installed function contract', () => {
  assert.match(
    sql,
    /CREATE\s+OR\s+REPLACE\s+FUNCTION\s+public\.pay_batch_freshness_scope_seed\(\s*p_operation_id\s+uuid,\s*p_pay_batch_id\s+uuid\s+DEFAULT\s+NULL::uuid,\s*p_cursor_json\s+jsonb\s+DEFAULT\s+NULL::jsonb,\s*p_limit\s+integer\s+DEFAULT\s+100\s*\)/i
  );
  assert.match(sql, /SECURITY\s+DEFINER/i);
  assert.match(sql, /SET\s+search_path\s+TO\s+'public'/i);
});

test('freshness scope seeding remains a service-role Worker authority', () => {
  assert.match(
    sql,
    /REVOKE\s+ALL\s+ON\s+FUNCTION\s+public\.pay_batch_freshness_scope_seed\(uuid, uuid, jsonb, integer\)\s+FROM\s+authenticated/i,
  );
  assert.match(
    sql,
    /GRANT\s+EXECUTE\s+ON\s+FUNCTION\s+public\.pay_batch_freshness_scope_seed\(uuid, uuid, jsonb, integer\)\s+TO\s+service_role/i,
  );
});
