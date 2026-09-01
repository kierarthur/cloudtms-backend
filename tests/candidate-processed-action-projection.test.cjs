const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const repeatable = fs.readFileSync(path.join(
  __dirname,
  '..',
  'supabase',
  'repeatable',
  '27082026_0244_candidate_processed_action_projection_v1.sql'
), 'utf8');

const authoritativeDailyAdapter = fs.readFileSync(path.join(
  __dirname,
  '..',
  'supabase',
  'repeatable',
  '28082026_2027_candidate_daily_office_receipt_adapter_v1.sql'
), 'utf8');

const verification = fs.readFileSync(path.join(
  __dirname,
  '..',
  'supabase',
  'verification',
  '27082026_0245_candidate_processed_action_projection_verification.sql'
), 'utf8');

test('processed Candidate hours are immutable while a separate expense claim remains available', () => {
  assert.match(repeatable, /v_candidate_mutation_locked:=v_fin\.authorised_at_utc is not null[\s\S]*PENDING_AUTH[\s\S]*READY_FOR_HR[\s\S]*READY_FOR_INVOICE/i);
  assert.match(repeatable, /'can_edit_hours'[\s\S]*not v_candidate_mutation_locked/i);
  assert.match(repeatable, /'can_edit_expenses'[\s\S]*v_candidate_mutation_locked/i);
  assert.match(repeatable, /'candidate_no_work_allowed'[\s\S]*not v_candidate_mutation_locked/i);
  assert.match(repeatable, /'candidate_no_work_allowed'[\s\S]*v_hours=0[\s\S]*v_additional=0[\s\S]*v_expenses=0/i);
  assert.match(repeatable, /'candidate_no_work_allowed'[\s\S]*not v_has_claim_evidence[\s\S]*not v_has_embedded_submission_evidence[\s\S]*not v_has_worked_schedule[\s\S]*not v_has_active_submission_workflow/i);
  assert.match(authoritativeDailyAdapter, /'candidate_no_work_allowed'[\s\S]*v_hours=0[\s\S]*v_additional=0[\s\S]*v_expenses=0/i);
  assert.match(authoritativeDailyAdapter, /'candidate_no_work_allowed'[\s\S]*not v_has_claim_evidence[\s\S]*not v_has_embedded_submission_evidence[\s\S]*not v_has_worked_schedule[\s\S]*not v_has_active_submission_workflow/i);
});

test('installed-state verification invokes the action contract and rejects resubmission actions', () => {
  assert.match(verification, /_candidate_record_capabilities_v1\(/i);
  assert.match(verification, /_candidate_timesheet_action_contract_v1\(/i);
  assert.match(verification, /primary_action,code[\s\S]*ADD_EXPENSES/i);
  assert.match(verification, /ENTER_TIMESHEET[\s\S]*NO_WORK_THIS_WEEK/i);
  assert.match(verification, /rollback;/i);
});

test('the replacement contains no illegally schema-qualified conditional construct', () => {
  assert.doesNotMatch(repeatable, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(authoritativeDailyAdapter, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
