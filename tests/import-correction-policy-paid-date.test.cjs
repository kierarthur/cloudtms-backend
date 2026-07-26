const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..');
const policyFile = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '21072026_1235_01_correction_financials_policy_resolve_v1.sql',
);
const body = fs.readFileSync(policyFile, 'utf8');

test('PAID_DATE prefers the exact current correction component lineage', () => {
  assert.match(
    body,
    /frozen_component_snapshot_json\s*->\s*'ordered_member_timesheet_ids'[\s\S]*\?\s*p_timesheet_id::text/,
  );
  assert.match(
    body,
    /payment_date_match_scope',\s*'CURRENT_COMPONENT_LINEAGE'/,
  );
  assert.match(
    body,
    /frozen_component_key_type[\s\S]*'TS_DAY'[\s\S]*frozen_component_key_value[\s\S]*v_shift\.work_date::text/,
  );
});

test('PAID_DATE retains a fail-closed component-scoped legacy fallback', () => {
  assert.match(body, /payment_date_match_scope',\s*'LEGACY_COMPONENT'/);
  assert.match(
    body,
    /if v_pay_batch_date_count > 1 then[\s\S]*CORRECTION_POLICY_COMPONENT_PAYMENT_DATE_AMBIGUOUS/,
  );
  assert.doesNotMatch(
    body,
    /where pbi\.timesheet_id = v_root_timesheet_id\s+and coalesce\(pbi\.is_voided, false\) = false;\s+[\s\S]{0,200}CORRECTION_POLICY_ROOT_PAYMENT_DATE_AMBIGUOUS/,
  );
});

test('frozen component payment date overrides the root TSFIN paid date', () => {
  assert.match(
    body,
    /v_root_paid_date := coalesce\(v_pay_batch_date, v_root_paid_date\);/,
  );
  assert.doesNotMatch(
    body,
    /v_root_paid_date := coalesce\(v_root_paid_date, v_pay_batch_date\);/,
  );
});
