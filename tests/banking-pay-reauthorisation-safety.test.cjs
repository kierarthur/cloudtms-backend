const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const overlaySql = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '12082026_1343_pay_payment_correction_reauthorisation_overlay_reset_v1.sql'), 'utf8');
const freshnessSql = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '12082026_1343_pay_batch_freshness_user_failures_v1.sql'), 'utf8');

test('financially complete correction retires only a proven local reauthorisation transfer overlay', () => {
  assert.match(worker, /phase === 'REFRESH_WORKBENCH'[\s\S]*result\.financial_complete === true[\s\S]*financialResult\.reauthorisation_required === true/);
  assert.match(worker, /pay_payment_correction_reauthorisation_overlay_reset_v1/);
  assert.match(overlaySql, /execution_commit_state[\s\S]*NOT_SUBMITTED/);
  assert.match(overlaySql, /transfer_row\.status[\s\S]*PENDING/);
  assert.match(overlaySql, /transfer_row\.rail_tx_id/);
  assert.match(overlaySql, /pay_bank_transfer_events/);
  assert.match(overlaySql, /banking_pay_operation_provider_attempts/);
  assert.match(overlaySql, /SET pay_bank_transfer_id = NULL::uuid/);
  assert.match(overlaySql, /status = 'VOIDED'/);
  assert.match(overlaySql, /policy_x_economics_changed/);
});

test('freshness presentation reuses exact scope-unit decisions and identifies affected frozen payments', () => {
  assert.match(freshnessSql, /decision_authority'[\s\S]*EXISTING_FRESHNESS_SCOPE_UNIT_RESULTS/);
  assert.match(freshnessSql, /banking_pay_operation_scope_units/);
  assert.match(freshnessSql, /unit_payload_json->>'pay_batch_item_id'/);
  assert.match(freshnessSql, /candidate_display_name/);
  assert.match(freshnessSql, /pay_batch_timesheet_snapshots/);
  assert.match(freshnessSql, /target_snapshot_json->>'client_name'/);
  assert.match(freshnessSql, /target_snapshot_json->>'week_ending_date'/);
  assert.match(freshnessSql, /current_timesheet\.week_ending_date/);
  assert.match(freshnessSql, /LEFT JOIN public\.contracts AS current_contract/);
  assert.match(freshnessSql, /LEFT JOIN public\.clients AS current_client/);
  assert.match(freshnessSql, /COALESCE\([\s\S]*frozen_timesheet[\s\S]*current_client\.name/);
  assert.match(freshnessSql, /member_row\.active_amount/);
  assert.match(freshnessSql, /pay_batch_paye_net_inputs/);
  assert.match(freshnessSql, /payment_amount_pence/);
  assert.match(freshnessSql, /reason_codes/);
  assert.match(freshnessSql, /Remove or resolve every payment listed, then reauthorise the remaining batch/);
  assert.match(worker, /pay_batch_freshness_user_failures_v1/);
  assert.match(worker, /stale_payment_failures/);
});
