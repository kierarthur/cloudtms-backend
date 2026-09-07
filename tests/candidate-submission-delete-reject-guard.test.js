import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL(
  '../supabase/repeatable/05092026_2100_candidate_submission_delete_reject_guard_v1.sql',
  import.meta.url
), 'utf8');
const pendingExpenseDeleteSql = readFileSync(new URL(
  '../supabase/repeatable/05092026_1515_pending_expense_timesheet_delete_v1.sql',
  import.meta.url
), 'utf8');
const deletedWeeklyAuditShapeMigration = readFileSync(new URL(
  '../supabase/migrations/05092026_2345_candidate_weekly_deleted_submission_audit_shape.sql',
  import.meta.url
), 'utf8');
const verification = readFileSync(new URL(
  '../supabase/verification/05092026_2110_candidate_submission_delete_reject_guard_verification.sql',
  import.meta.url
), 'utf8');
const broker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const release = readFileSync(new URL('../supabase/release/current-release.json', import.meta.url), 'utf8');

test('Electronic and QR Candidate submissions require rejection before deletion', () => {
  assert.match(sql, /v_route_family in \('ELECTRONIC','QR'\)/i);
  assert.match(sql, /candidate_submission_stage'[\s\S]*?'MANAGER_APPROVED'[\s\S]*?'CANDIDATE_SUBMITTED'/i);
  assert.match(sql, /CANDIDATE_SUBMISSION_REJECTION_REQUIRED/i);
  assert.match(sql, /timesheet_delete_with_candidate_submission_guard_apply_v1/i);
  assert.match(verification, /candidate_submission_stage'<>'MANAGER_APPROVED'/i);
  assert.match(verification, /candidate-qr-delete-hours/i);
});

test('manual non-QR and import-authoritative hours stay outside the Candidate guard', () => {
  assert.match(sql, /'delete_guard_applicable'[\s\S]*?workflow_row\.workflow_kind='CONTRACT_EXPENSE'[\s\S]*?or v_route_family in \('ELECTRONIC','QR'\)/i);
  assert.match(verification, /Manual non-QR hours were incorrectly put behind Candidate rejection/i);
  assert.match(verification, /Import-authoritative NHSP hours were incorrectly gated/i);
  assert.match(verification, /weekly_timesheet_source[\s\S]*?'NHSP'/i);
});

test('contract-free Daily PHONE rejection stays available without entering the delete guard', () => {
  assert.match(sql, /v_timesheet\.sheet_scope='DAILY'::public\.timesheet_scope_enum[\s\S]*?v_route_family:='PHONE'/i);
  assert.match(sql, /workflow_row\.anchor_timesheet_id=v_timesheet\.timesheet_id/i);
  assert.match(sql, /coalesce\(\(v_target->>'delete_guard_applicable'\)::boolean,false\)[\s\S]*?candidate_submission_stage'<>'DRAFT'/i);
});

test('Candidate-created expense carriers remain protected even when stored in manual shape', () => {
  assert.match(sql, /workflow_row\.target_timesheet_id=v_timesheet\.timesheet_id[\s\S]*?workflow_row\.workflow_kind='CONTRACT_EXPENSE'/i);
  assert.match(verification, /manual-shaped-candidate-expense/i);
  assert.match(verification, /Candidate-created expense carrier was not protected/i);
});

test('a target-less pending expense follows an equivalent rotated Timesheet identity', () => {
  assert.match(sql, /anchor_timesheet\.booking_id is not distinct from v_timesheet\.booking_id/i);
  assert.match(sql, /anchor_timesheet\.occupant_key_norm[\s\S]*?is not distinct from v_timesheet\.occupant_key_norm/i);
  assert.match(sql, /anchor_timesheet\.contract_id is not distinct from v_timesheet\.contract_id/i);
  assert.match(sql, /anchor_timesheet\.week_ending_date is not distinct from v_timesheet\.week_ending_date/i);
  assert.match(sql, /'ROTATED_ANCHOR'/i);
  assert.match(verification, /linked_pending_expense_claim_count/i);
  assert.match(verification, /rejection_scope[\s\S]*?'COMPLETE_EXPENSE_CLAIM'/i);
});

test('a Weekly delete set resolves an older Timesheet version through the current route', () => {
  assert.match(sql, /v_week\.id is null[\s\S]*?_candidate_office_projection_identity_v1\([\s\S]*?v_timesheet\.timesheet_id,null[\s\S]*?_candidate_route_family_v1\([\s\S]*?v_route_timesheet_id,v_route_contract_week_id/i);
  assert.match(verification, /array\[v_old_timesheet,v_timesheet\]/i);
  assert.match(verification, /full rotated Timesheet chain/i);
  assert.match(verification, /workflow_id'=v_expense_workflow::text\)<>1/i);
});

test('preview, rejection and final delete use the same guarded workflow authority', () => {
  assert.match(sql, /_candidate_office_reject_preview_v1[\s\S]*?_candidate_office_rejection_targets_v2/i);
  assert.match(sql, /candidate_submission_reject_atomic_v1[\s\S]*?_candidate_office_rejection_targets_v2/i);
  assert.match(sql, /timesheet_delete_with_candidate_submission_guard_apply_v1[\s\S]*?_timesheet_candidate_submission_delete_guard_v1/i);
  assert.match(sql, /timesheet_delete_with_candidate_submission_guard_apply_v1[\s\S]*?timesheet_delete_with_pending_expense_apply_v1/i);
  assert.match(verification, /Delete refusal changed the Timesheet or pending expense/i);
  assert.match(verification, /Atomic rejection did not reject both workflows exactly/i);
});

test('final delete retains terminal Candidate history without retaining live Timesheet links', () => {
  assert.match(sql, /_candidate_timesheet_delete_retire_workflows_v1/i);
  assert.match(sql, /'CREATED','WORKER_DRAFT','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'/i);
  assert.match(sql, /CANDIDATE_SUBMISSION_REJECTION_REQUIRED/i);
  assert.match(sql, /set timesheet_id=null,[\s\S]*?'type','workflow','workflow_id',v_workflow\.id/i);
  assert.match(sql, /contract_week_id=null,[\s\S]*?anchor_timesheet_id=null,[\s\S]*?target_timesheet_id=null/i);
  assert.match(sql, /office_permanent_delete_tombstone/i);
  assert.match(sql, /CANDIDATE_WORKFLOW_RETAINED_AFTER_TIMESHEET_DELETE/i);
  assert.match(sql, /v_pre_retirement_row_signature[\s\S]*?ROW_SIGNATURE_MISMATCH/i);
  assert.match(sql, /v_post_retirement_row_signature[\s\S]*?timesheet_delete_with_pending_expense_apply_v1/i);
  assert.match(sql, /p_expected_timesheet_id,v_post_retirement_row_signature,p_expected_timesheet_ids/i);
  assert.match(verification, /Rejected replacement did not complete one safe permanent delete/i);
  assert.match(verification, /state='REJECTED'[\s\S]*?contract_week_id is null/i);
  assert.match(verification, /event_type='OFFICE_REJECTED'[\s\S]*?deep_link_json->>'type'='workflow'/i);
});

test('the deleted-week audit shape is terminal-only and does not relax active weekly identity', () => {
  assert.match(deletedWeeklyAuditShapeMigration, /workflow_kind in \('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED'\)/i);
  assert.match(deletedWeeklyAuditShapeMigration, /contract_id is not null[\s\S]*?week_ending_date is not null/i);
  assert.match(deletedWeeklyAuditShapeMigration, /contract_week_id is not null[\s\S]*?or \([\s\S]*?state in \('REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'\)/i);
  assert.match(deletedWeeklyAuditShapeMigration, /contract_week_id is null[\s\S]*?target_timesheet_id is null[\s\S]*?anchor_timesheet_id is null/i);
  assert.match(deletedWeeklyAuditShapeMigration, /OFFICE_PERMANENTLY_DELETED_TIMESHEET/i);
  assert.match(deletedWeeklyAuditShapeMigration, /OFFICE_PERMANENTLY_DELETED_DAILY_RECEIPT/i);
});

test('direct pending-expense cancellation records the same safe deleted-week tombstone', () => {
  assert.match(pendingExpenseDeleteSql, /contract_week_id=null,[\s\S]*?anchor_timesheet_id=null,[\s\S]*?target_timesheet_id=null/i);
  assert.match(pendingExpenseDeleteSql, /office_permanent_delete_tombstone/i);
  assert.match(pendingExpenseDeleteSql, /OFFICE_TIMESHEET_DELETED_EXPENSE_CANCELLED/i);
  assert.match(pendingExpenseDeleteSql, /OFFICE_PERMANENTLY_DELETED_TIMESHEET/i);
  assert.match(pendingExpenseDeleteSql, /candidate-expense-claim-cancelled-timesheet-delete-v1/i);
});

test('linked expense rejection emits its own accurate push-ready notification', () => {
  assert.match(sql, /linked_pending_expense[\s\S]*?'EXPENSE_CLAIM_CANCELLED'[\s\S]*?'timesheet_expense_attention'/i);
  assert.match(sql, /candidate-expense-claim-cancelled-timesheet-rejection-v1/i);
  assert.match(sql, /LINKED_TIMESHEET_REJECTED_FOR_DELETE/i);
  assert.match(sql, /jsonb_build_object\('type','workflow','workflow_id',v_workflow\.id\)/i);
  assert.match(verification, /event_type='EXPENSE_CLAIM_CANCELLED'[\s\S]*?template_key='candidate-expense-claim-cancelled-timesheet-rejection-v1'/i);
});

test('Office validates and presents the reject-before-delete result', () => {
  assert.match(broker, /function loadTimesheetCandidateSubmissionDeleteGuard/i);
  assert.match(broker, /TIMESHEET_CANDIDATE_SUBMISSION_DELETE_GUARD_V1/i);
  assert.match(broker, /timesheet_delete_with_candidate_submission_guard_apply_v1/i);
  assert.match(broker, /This Timesheet has been approved by the manager and must be rejected before it can be deleted/i);
  assert.match(broker, /Rejecting it will also reject.*linked pending expense claim/s);
});

test('new service-only functions remain inaccessible to browser roles and mandatory in releases', () => {
  for (const role of ['anon', 'authenticated']) {
    assert.match(verification, new RegExp(`has_function_privilege\\(\\s*'${role}'`, 'i'));
  }
  assert.match(sql, /grant execute on function public\.timesheet_candidate_submission_delete_guard_preview_v1\(text,uuid\[\]\)[\s\S]*?to service_role/i);
  assert.match(sql, /grant execute on function public\.timesheet_delete_with_candidate_submission_guard_apply_v1[\s\S]*?to service_role/i);
  assert.equal((release.match(/05092026_2110_candidate_submission_delete_reject_guard_verification\.sql/g) || []).length, 2);
});
