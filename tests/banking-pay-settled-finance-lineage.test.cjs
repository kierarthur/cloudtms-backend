const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const authorityPath = path.join(
  root,
  'supabase',
  'repeatable',
  '11082026_1746_pay_active_settled_components_finance_lineage.sql',
);
const authority = fs.readFileSync(authorityPath, 'utf8');
const reassert = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '08082026_0902_reassert_authorities_after_legacy_monolith.sql',
), 'utf8');

test('settled finance movement can resolve its immutable component timesheet lineage', () => {
  assert.match(
    authority,
    /LEFT JOIN public\.pay_finance_case_components AS finance_component\s+ON finance_component\.id = finance_item\.finance_component_id/,
  );
  assert.match(
    authority,
    /WHEN finance_component\.linked_timesheet_id IS NOT NULL\s+THEN finance_component\.linked_timesheet_id/,
  );
  assert.match(
    authority,
    /WHEN finance_item\.timesheet_id IS NOT NULL THEN finance_item\.timesheet_id[\s\S]*frozen_source_basis_json[\s\S]*finance_component\.linked_timesheet_id/,
  );
});

test('settled finance lineage authority preserves the installed object contract', () => {
  assert.match(authority, /LANGUAGE sql\s+STABLE\s+SECURITY DEFINER\s+SET search_path TO 'public'/);
  assert.match(
    authority,
    /REVOKE ALL ON FUNCTION public\._pay_active_settled_components\(uuid\[\]\) FROM PUBLIC, anon/,
  );
  assert.match(
    authority,
    /GRANT EXECUTE ON FUNCTION public\._pay_active_settled_components\(uuid\[\]\) TO authenticated, service_role, postgres/,
  );
});

test('legacy omnibus replay restores the focused settled finance lineage authority', () => {
  assert.match(
    reassert,
    /\\ir 11082026_1746_pay_active_settled_components_finance_lineage\.sql/,
  );
});
