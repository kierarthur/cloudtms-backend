const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const sql = fs.readFileSync(
  path.resolve(
    __dirname,
    '../supabase/repeatable/26072026_1519_pay_batch_validate_freshness_correction_chain_wrapper.sql'
  ),
  'utf8'
);

test('freshness keeps the base validator fail-closed', () => {
  assert.match(sql, /_pay_batch_validate_freshness_base_v1\s*\(/);
  assert.doesNotMatch(sql, /RETURN\s+jsonb_build_object\(\s*'is_stale'\s*,\s*false/i);
  assert.match(sql, /v_remaining_key_diff_count\s*=\s*0[\s\S]*RESERVATION_CHANGED/);
});

test('settled-baseline recovery requires one exact current semantic V3 publication', () => {
  assert.match(sql, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3/);
  assert.match(sql, /certified_preview_publication_parity_ok/);
  assert.match(sql, /certified_preview_publication_source_publication_id/);
  assert.match(sql, /certified_preview_publication_source_build_run_id/);
  assert.match(sql, /certified_preview_publication_source_change_seq[\s\S]*registry\.current_source_change_seq/);
  assert.match(sql, /registry\.dirty_generation\s*=\s*registry\.evaluated_generation/);
  assert.match(sql, /economic_build\.captured_candidate_generation[\s\S]*registry\.evaluated_generation/);
  assert.match(sql, /economic_build\.status\s*=\s*'COMPLETE'/);
  assert.match(sql, /current_source\.status\s*=\s*'CURRENT'/);
  assert.match(sql, /semantic_ready/);
  assert.match(sql, /invalid_selectable_row_count/);
  assert.match(sql, /WHERE\s+1\s*=\s*\([\s\S]*COUNT\(\*\)[\s\S]*candidate_authority\.candidate_id\s*=\s*authority\.candidate_id/);
});

test('settled-baseline recovery reconciles certified truth against live truth and other reservations', () => {
  assert.match(sql, /pay_current_timesheet_entitlement_components_from_build_v1\s*\(/);
  assert.match(sql, /_pay_current_timesheet_entitlement_components\s*\(/);
  assert.match(sql, /_pay_outstanding_components\s*\([\s\S]*p_pay_batch_id/);
  assert.match(sql, /live_truth_component\.truth_ex_vat[\s\S]*certified_component\.truth_ex_vat/);
  assert.match(sql, /live_truth_component\.baseline_ex_vat[\s\S]*certified_component\.baseline_ex_vat[\s\S]*>\s*0\.01/);
  assert.match(
    sql,
    /certified_component\.truth_ex_vat[\s\S]*-\s*COALESCE\(certified_component\.baseline_ex_vat[\s\S]*-\s*COALESCE\(live_component\.reserved_ex_vat[\s\S]*frozen_item\.frozen_source_ex/
  );
  assert.match(sql, /frozen_item\.frozen_target_ex\s*-\s*frozen_item\.physical_target_ex/);
});

test('settled-baseline recovery excludes correction and already-applied cancellation items', () => {
  assert.match(sql, /NOT LIKE 'correction-chain:%'/);
  assert.match(sql, /case_resolution_summary[\s\S]*IS DISTINCT FROM 'object'/);
  assert.match(sql, /pay_payment_correction_items/);
  assert.match(sql, /'PRE_BANK_CANCEL'[\s\S]*'NO_MONEY_UNWIND'[\s\S]*'SETTLED_REVERSAL'/);
});
