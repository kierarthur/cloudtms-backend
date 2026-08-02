const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const read = (relativePath) => fs.readFileSync(path.resolve(__dirname, '..', relativePath), 'utf8');
const preview = read('supabase/repeatable/02082026_2014_timesheet_correction_pair_lifecycle_preview_v1.sql');
const policy = read('supabase/repeatable/21072026_1235_00_import_correction_policy_helpers.sql');
const runtime = read('supabase/repeatable/21072026_1235_00b_import_correction_runtime_guards.sql');
const scope = read('supabase/repeatable/21072026_1235_10_invoice_correction_pair_scope_v1.sql');
const authorise = read('supabase/repeatable/21072026_1235_32_timesheet_authorise_bulk_atomic.sql');
const unauthorise = read('supabase/repeatable/21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql');
const review = read('supabase/repeatable/21072026_1820_00_import_review_internal_core.sql');
const invoiceEdit = read('supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_apply_edits.sql');
const invoiceValidation = read('supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_correction_validate_batch.sql');
const lifecycle = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
const worker = read('broker/src/index.js');

test('one selected correction member expands to one exact two-member lifecycle group', () => {
  assert.match(preview, /timesheet_correction_pair_transition_v1\(/i);
  assert.match(preview, /expected_member_count'\)::integer,0\)=2/i);
  assert.match(preview, /pair_fingerprint/i);
  assert.match(policy, /timesheet_correction_pair_lifecycle_preview_v1/i);
  assert.match(policy, /expected_pair_fingerprint/i);
  assert.match(policy, /CORRECTION_UNIT_LIFECYCLE_PREVIEW_STALE/i);
});

test('authorise and unauthorise apply both pair members atomically', () => {
  for (const sql of [authorise, unauthorise]) {
    assert.match(sql, /_ctms_expand_lifecycle_items_v1/i);
    assert.match(sql, /CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED/i);
    assert.match(sql, /CORRECTION_PAIR_LIFECYCLE_POSTCONDITION_FAILED/i);
    assert.match(sql, /affected_timesheet_ids/i);
  }
  assert.match(worker, /CORRECTION_PAIR_CONFIRMATION_REQUIRED/g);
  assert.match(worker, /runCorrectionPairLifecycleIfApplicable/i);
  assert.match(worker, /timesheet_authorise_bulk_atomic/i);
  assert.match(worker, /timesheet_unauthorise_bulk_atomic/i);
});

test('delete archive and unarchive resolve the exact pair rather than a parent chain', () => {
  assert.match(lifecycle, /correction-pair-removal-v1/i);
  assert.match(lifecycle, /CORRECTION_PAIR_MUST_BE_UNAUTHORISED/i);
  assert.match(lifecycle, /CORRECTION_PAIR_HAS_LATER_GENERATION/i);
  assert.match(lifecycle, /PAIR_PARTIALLY_ARCHIVED_BLOCKED/i);
  assert.match(lifecycle, /CORRECTION_PAIR_UNARCHIVE_SUPERSEDED/i);
  assert.match(lifecycle, /'pair_timesheet_ids'/i);
});

test('invoice placement supports compatible complete splits and one bounded move gap', () => {
  for (const state of [
    'UNPLACED', 'COMPLETE_SAME_INVOICE', 'COMPLETE_SPLIT_INVOICES',
    'INCOMPLETE_MOVE', 'DUPLICATE_PLACEMENT', 'INCOMPATIBLE_PLACEMENT', 'MALFORMED_PAIR'
  ]) assert.match(scope, new RegExp(state));
  assert.match(runtime, /INVOICE_APPLY_EDITS_RESULT[\s\S]*INCOMPLETE_MOVE[\s\S]*UNPLACED/i);
  assert.match(invoiceEdit, /v_correction_placement_only/i);
  assert.match(invoiceEdit, /IMPORT_CORRECTION_PLACEMENT_ONLY_TARGET_INVALID/i);
  assert.match(invoiceEdit, /INVOICE_CORRECTION_PAIR_PLACEMENT_MOVE_NOT_STARTABLE/i);
  assert.match(invoiceEdit, /INVOICE_CORRECTION_PAIR_PLACEMENT_TARGET_INVALID/i);
  assert.match(invoiceValidation, /INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE/i);
});

test('Import Review partitions issued and draft economics without double counting', () => {
  assert.match(review, /B_issued_hours/i);
  assert.match(review, /P_pending_hours/i);
  assert.match(review, /planned_invoice_hours/i);
  assert.match(review, /v_component_economic_state:=case[\s\S]*'PENDING'[\s\S]*'EFFECTIVE'/i);
  assert.match(review, /IMPORT_REVIEW_CORRECTION_PAIR_PLACEMENT_INCOMPLETE/i);
  assert.match(review, /v_pending_line_ids::text/i);
});

test('the validation catalogue removes only the redundant invoiced email row', () => {
  assert.match(review, /owner_row\.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'/i);
  assert.match(review, /email_row\.action_kind in \('EMAIL_ISSUE','EMAIL_REMINDER'\)/i);
  assert.match(review, /delete from pg_temp\.import_review_catalog_v1 email_row/i);
  assert.match(review, /'delivery_history_status',email_row\.summary_json->>'delivery_history_status'/i);
});

test('timesheet summary marks only the unplaced correction member', () => {
  assert.match(worker, /enrichCorrectionPairPlacementIssues/i);
  assert.match(worker, /placedMembers\.length !== 1/i);
  assert.match(worker, /Paired needs invoicing/i);
});

test('the targeted implementation does not introduce Banking Pay or Daily mutation into pair SQL', () => {
  const targeted = [preview, policy, scope, authorise, unauthorise, invoiceEdit, invoiceValidation];
  for (const sql of targeted) {
    assert.doesNotMatch(sql, /pay_workbench|pay_execute_bank|settlement|remittance/i);
    assert.doesNotMatch(sql, /hr_daily_apply_transactional/i);
  }
});
