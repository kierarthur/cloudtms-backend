import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL(
  '../supabase/repeatable/10092026_0823_planned_week_candidate_submission_delete_guard.sql',
  import.meta.url
), 'utf8');
const broker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const candidateBackend = readFileSync(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');

test('planned Contract Weeks report submitted and manager-approved Candidate workflows', () => {
  assert.match(sql, /contract_week_submission_delete_guard_preview_v1/i);
  assert.match(sql, /'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL'/i);
  assert.match(sql, /'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT'/i);
  assert.match(sql, /'candidate_submission_stage',v_stage/i);
  assert.match(sql, /'CANDIDATE_SUBMITTED'|CANDIDATE_SUBMITTED/i);
  assert.match(sql, /'MANAGER_APPROVED'|MANAGER_APPROVED/i);
  assert.match(sql, /'context_sha256',v_context_sha/i);
});

test('Office rejection returns the planned week to a blank Candidate claim and notifies the Candidate', () => {
  assert.match(sql, /contract_week_submission_reject_atomic_v1/i);
  assert.match(sql, /candidate_must_start_new_claim',true/i);
  assert.match(sql, /update public\.contract_weeks[\s\S]*?status='OPEN'/i);
  assert.match(sql, /'OFFICE_REJECTED',p_now_utc/i);
  assert.match(sql, /CANDIDATE_SUBMISSION_REJECTED/i);
  assert.match(sql, /where id=p_contract_week_id and timesheet_id is null/i);
});

test('planned deletion is race-safe and rejection is mandatory before mutation', () => {
  assert.match(sql, /contract_week_delete_planned_guarded_v1/i);
  assert.match(sql, /CANDIDATE_SUBMISSION_REJECTION_REQUIRED/i);
  assert.match(sql, /CONTRACT_WEEK_(?:SUBMISSION_)?(?:DELETE_)?CONTEXT_CHANGED/i);
  assert.match(sql, /from public\.contract_week_delete_planned\(/i);
  assert.match(broker, /loadContractWeekSubmissionDeleteGuard/i);
  assert.match(broker, /candidate_submission_rejection_required === true/i);
  assert.match(broker, /expected_context_sha256 must be a SHA-256 value/i);
  assert.match(broker, /delete_operation_id must be a valid UUID/i);

  const start = broker.indexOf('async function handleContractWeekDeletePlanned');
  const end = broker.indexOf('async function handleTimesheetAuditFeed', start);
  const handler = broker.slice(start, end);
  const guardedDelete = handler.indexOf("'contract_week_delete_planned_guarded_v1'");
  const cleanup = handler.lastIndexOf('deleteContractWeekStagedFilesAndRows(contractWeek)');
  assert.ok(guardedDelete >= 0 && cleanup > guardedDelete,
    'staged files must be cleaned only after the guarded database delete succeeds');
  assert.doesNotMatch(handler, /rest\/v1\/rpc\/contract_week_delete_planned[`'\"]/i);
});

test('Office exposes narrow planned-week rejection routes without widening Timesheet projection', () => {
  assert.match(candidateBackend, /handleOfficeContractWeekRejectPreview/i);
  assert.match(candidateBackend, /handleOfficeContractWeekReject\(/i);
  assert.match(candidateBackend, /contract_week_submission_delete_guard_preview_v1/i);
  assert.match(candidateBackend, /contract_week_submission_reject_atomic_v1/i);
  assert.match(candidateBackend, /\/api\/candidate-app\/contract-weeks\/:contractWeekId\/reject-preview/i);
  assert.match(candidateBackend, /\/api\/candidate-app\/contract-weeks\/:contractWeekId\/reject/i);
  assert.match(candidateBackend, /requireOfficeActor\(request, deps, 'reject_submission'\)/i);
});

test('all new database functions are service-only', () => {
  assert.match(sql, /revoke all on function public\.contract_week_submission_delete_guard_preview_v1[\s\S]*?from public,anon,authenticated/i);
  assert.match(sql, /grant execute on function public\.contract_week_submission_delete_guard_preview_v1[\s\S]*?to service_role/i);
  assert.match(sql, /grant execute on function public\.contract_week_submission_reject_atomic_v1[\s\S]*?to service_role/i);
  assert.match(sql, /grant execute on function public\.contract_week_delete_planned_guarded_v1[\s\S]*?to service_role/i);
});
