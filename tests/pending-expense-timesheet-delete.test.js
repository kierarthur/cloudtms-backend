import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL(
  '../supabase/repeatable/05092026_1515_pending_expense_timesheet_delete_v1.sql',
  import.meta.url
), 'utf8');
const broker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const candidateBackend = readFileSync(new URL(
  '../broker/src/candidate-app-backend.js', import.meta.url
), 'utf8');
const verification = readFileSync(new URL(
  '../supabase/verification/05092026_1530_pending_expense_timesheet_delete_verification.sql',
  import.meta.url
), 'utf8');
const bankingDirtyTrigger = readFileSync(new URL(
  '../supabase/repeatable/04082026_1219_pay_workbench_mark_candidate_dirty.sql',
  import.meta.url
), 'utf8');
const releaseManifest = readFileSync(new URL(
  '../supabase/release/current-release.json',
  import.meta.url
), 'utf8');

test('only unfinished standalone expense claims without a Timesheet are attached to deletion', () => {
  assert.match(sql, /workflow_row\.workflow_kind='CONTRACT_EXPENSE'/i);
  assert.match(sql, /workflow_row\.target_timesheet_id is null/i);
  assert.match(sql, /workflow_row\.state not in \('FINALISED','CANCELLED','REJECTED','EXPIRED','SUPERSEDED'\)/i);
  assert.match(sql, /delete_timesheet\.timesheet_id=anchor_timesheet\.timesheet_id/i);
  assert.match(sql, /delete_timesheet\.booking_id is not distinct from anchor_timesheet\.booking_id[\s\S]*?delete_timesheet\.occupant_key_norm=anchor_timesheet\.occupant_key_norm[\s\S]*?delete_timesheet\.contract_id is not distinct from anchor_timesheet\.contract_id[\s\S]*?delete_timesheet\.week_ending_date is not distinct from anchor_timesheet\.week_ending_date/i);
  assert.doesNotMatch(sql, /workflow_row\.workflow_kind='CONTRACT_COMBINED'/i);
});

test('manager approval and carrier inconsistencies block deletion instead of cancelling', () => {
  assert.match(sql, /approval\.item->>'state'='APPROVED'/i);
  assert.match(sql, /PENDING_EXPENSE_MANAGER_APPROVAL_ALREADY_RECORDED/i);
  assert.match(sql, /PENDING_EXPENSE_CARRIER_INCONSISTENT/i);
  assert.match(sql, /PENDING_EXPENSE_DELETE_BLOCKED/i);
  assert.match(verification, /set state='MANAGER_APPROVED'/i);
  assert.match(verification, /blocking_claim_count/i);
});

test('cancellation and every established Timesheet delete path share one transaction', () => {
  assert.match(sql, /create or replace function public\.timesheet_delete_with_pending_expense_apply_v1/i);
  assert.match(sql, /public\.candidate_workflow_cancel_atomic_v2/i);
  assert.match(sql, /public\.timesheet_standard_delete_apply_v1/i);
  assert.match(sql, /public\.timesheet_daily_abandoned_receipt_delete_apply_v1/i);
  assert.match(sql, /public\.timesheet_weekly_chain_delete_apply/i);
  assert.match(sql, /public\.timesheet_weekly_manual_adjustment_delete_apply/i);
  assert.match(sql, /TIMESHEET_DELETE_AFTER_EXPENSE_CANCELLATION_NOT_PROVEN/i);
  assert.match(sql, /^begin;[\s\S]*commit;\s*$/i);
  assert.doesNotMatch(sql, /delete\s+from\s+public\.candidate_submission_workflows/i);
});

test('Timesheet deletion produces one candidate-wide Banking refresh after the row is gone', () => {
  assert.match(sql, /cardinality\(v_delete_candidate_ids\)<>1/i);
  assert.match(sql, /pg_advisory_xact_lock\(v_candidate_lock_key\)/i);
  assert.match(sql, /create temp table pg_temp\._bpay_candidate_delete_context_v1/i);
  assert.match(sql, /drop table pg_temp\._bpay_candidate_delete_context_v1/i);
  assert.match(sql, /p_targeted_timesheet_ids=>array\[\]::uuid\[\]/i);
  assert.match(sql, /'scope_resolution','CANDIDATE_FULL_AFTER_TIMESHEET_DELETE'/i);
  assert.match(sql, /TIMESHEET_DELETE_BANKING_REFRESH_NOT_PROVEN/i);
  assert.match(verification, /banking_pay_candidate_refresh,ok/i);
  assert.match(
    bankingDirtyTrigger,
    /deleted_timesheet_contract\.candidate_id[\s\S]*deleted_timesheet_contract[\s\S]*v_old_row->>'contract_id'/i
  );
});

test('cancelled claim is detached from the deleted Timesheet and produces one push-ready notice', () => {
  assert.match(sql, /set contract_week_id=null,[\s\S]*?anchor_timesheet_id=null,[\s\S]*?target_timesheet_id=null/i);
  assert.match(sql, /office_permanent_delete_tombstone/i);
  assert.match(sql, /state='CANCELLED'/i);
  assert.match(sql, /OFFICE_TIMESHEET_DELETED_EXPENSE_CANCELLED/i);
  assert.match(sql, /'EXPENSE_CLAIM_CANCELLED','timesheet_expense_attention'/i);
  assert.match(sql, /then 'PENDING'[\s\S]*?else 'SKIPPED'/i);
  assert.match(sql, /on conflict\(dedupe_key\)/i);
  assert.match(candidateBackend, /EXPENSE_CLAIM_CANCELLED:\s*'Your pending expense claim was cancelled because its Timesheet was deleted\.'/i);
});

test('browser roles cannot call the new deletion authority', () => {
  for (const role of ['anon', 'authenticated']) {
    assert.match(sql, new RegExp(
      `revoke all on function public\\.timesheet_pending_expense_delete_preview_v1\\(text,uuid\\[\\]\\)[\\s\\S]*?from public,anon,authenticated`,
      'i'
    ));
    assert.match(verification, new RegExp(`has_function_privilege\\('${role}'`, 'i'));
  }
  assert.match(sql, /grant execute on function public\.timesheet_pending_expense_delete_preview_v1\(text,uuid\[\]\)[\s\S]*?to service_role/i);
  assert.match(sql, /notify pgrst\s*,\s*'reload schema'/i);
});

test('Office uses one context digest from preview through apply and retires manager routes after commit', () => {
  assert.match(broker, /timesheet_pending_expense_delete_preview_v1/);
  assert.match(broker, /expected_pending_expense_context_sha256/);
  assert.match(broker, /timesheet_delete_with_candidate_submission_guard_apply_v1/);
  assert.match(broker, /resultMismatchFields\.push\('banking_pay_candidate_refresh'\)/);
  assert.match(broker, /banking_pay_candidate_refresh_queued: true/);
  assert.match(broker, /loadCandidateManagerRouteTicketsForWorkflows/);
  assert.match(broker, /retireCandidateManagerRoutesAfterTimesheetDelete\(env, managerRouteTickets, ctx\)/);
  assert.match(broker, /target_state: 'RETIRED'/);
  assert.doesNotMatch(
    broker.slice(
      broker.indexOf('let applyResult;', broker.indexOf('async function handleTimesheetDelete')),
      broker.indexOf('if (!applyResult ||', broker.indexOf('async function handleTimesheetDelete'))
    ),
    /sbRpc\(env, 'timesheet_(?:standard|daily_abandoned|weekly_chain|weekly_manual_adjustment)_delete_apply/
  );
});

test('Office returns every new deterministic deletion rejection as a safe error code', () => {
  for (const code of [
    'PENDING_EXPENSE_DELETE_APPLY_INVALID',
    'PENDING_EXPENSE_DELETE_CONTEXT_TOO_LARGE',
    'PENDING_EXPENSE_DELETE_TARGET_SET_INVALID',
    'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID',
    'PAY_WORKBENCH_CANDIDATE_DELETE_CONTEXT_CONFLICT',
    'TIMESHEET_DELETE_BANKING_REFRESH_NOT_PROVEN',
    'TIMESHEET_DELETE_CANDIDATE_OWNERSHIP_INVALID'
  ]) {
    assert.match(broker, new RegExp(`'${code}'`));
  }
});

test('rollback proof covers cancellation, rotation, manager approval and separate Timesheet exclusion', () => {
  assert.match(verification, /^begin;/im);
  assert.match(verification, /timesheet_delete_with_pending_expense_apply_v1/i);
  assert.match(verification, /update public\.timesheets set is_current=false/i);
  assert.match(verification, /target_timesheet_id=v_expense_timesheet/i);
  assert.match(verification, /event_type='EXPENSE_CLAIM_CANCELLED'/i);
  assert.match(verification, /push_state='PENDING'/i);
  assert.match(releaseManifest, /05092026_1530_pending_expense_timesheet_delete_verification\.sql/i);
  assert.match(verification, /rollback;\s*$/i);
});
