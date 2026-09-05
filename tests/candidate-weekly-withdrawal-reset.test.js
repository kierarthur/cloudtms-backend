import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const workflow = fs.readFileSync(path.join(root,
  'supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql'), 'utf8');
const reset = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_1255_candidate_weekly_withdrawal_reset_v1.sql'), 'utf8');
const nullTargetReset = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_2004_candidate_weekly_withdrawal_null_target_v1.sql'), 'utf8');
const officeReject = fs.readFileSync(path.join(root,
  'supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql'), 'utf8');
const candidateRead = fs.readFileSync(path.join(root,
  'supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql'), 'utf8');
const routeCapabilities = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_0244_candidate_processed_action_projection_v1.sql'), 'utf8');
const dailyVerification = fs.readFileSync(path.join(root,
  'supabase/verification/27082026_1327_candidate_daily_withdrawal_reset_verification.sql'), 'utf8');
const nullTargetVerification = fs.readFileSync(path.join(root,
  'supabase/verification/27082026_2005_candidate_weekly_null_target_withdrawal_verification.sql'), 'utf8');
const finalAuthority = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_1436_candidate_withdrawal_read_authority_v1.sql'), 'utf8');
const currentDetail = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_0858_candidate_finalised_artifact_readiness_v1.sql'), 'utf8');

test('withdrawal authority restores the final Timesheet-card expense projection', () => {
  assert.match(finalAuthority,
    /\\ir 27082026_0858_candidate_finalised_artifact_readiness_v1\.sql[\s\S]*\\ir 27082026_2350_candidate_timesheet_card_base_expense_fallback_v1\.sql[\s\S]*\\ir 28082026_0214_candidate_manager_refusal_resubmission_v1\.sql/i);
});

test('candidate cancellation retires approval authority and resets weekly hours atomically', () => {
  assert.match(workflow,
    /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*_candidate_manager_mail_retire_v1[\s\S]*_candidate_weekly_withdrawal_reset_v1[\s\S]*submission_withdrawal_reset/i);
  assert.match(reset,
    /scope<>'WEEKLY'[\s\S]*workflow_kind not in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)/i);
  assert.match(reset,
    /authorised_at_server is not null[\s\S]*authorised_at_utc is not null[\s\S]*paid_at_utc is not null[\s\S]*locked_by_invoice_id is not null/i);
});

test('manager-approved finalised submissions remain withdrawable until Office protection begins', () => {
  const actionContract = candidateRead.slice(
    candidateRead.indexOf('create or replace function private._candidate_timesheet_action_contract_v1')
  );
  assert.match(workflow,
    /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*v_action='SUPERSEDE' and v_workflow\.state='FINALISED'[\s\S]*_candidate_weekly_withdrawal_reset_v1/i);
  assert.match(workflow,
    /v_action in \('CANCEL','SUPERSEDE'\) then\s+if v_workflow\.state in \('CANCELLED','REJECTED','SUPERSEDED'\)\s+or \(v_action='SUPERSEDE' and v_workflow\.state='FINALISED'\)/i);
  assert.match(actionContract,
    /'AWAITING_PAPER_RETURN','RECEIVED','REFUSED','FINALISED'/i);
  assert.match(actionContract,
    /p_candidate_status_code[\s\S]*not in \('PAID','AUTHORISED','INVOICED_NOT_PAID'\)/i);
});

test('import-authoritative timesheets remain view-only and can never enter the withdrawal path', () => {
  assert.match(routeCapabilities,
    /'can_edit_hours',[\s\S]*not v_import/i);
  assert.match(workflow,
    /v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'[\s\S]*v_workflow_kind<>'CONTRACT_EXPENSE'[\s\S]*CANDIDATE_RECORD_VIEW_ONLY/i);
  assert.match(workflow,
    /v_action='CANCEL'[\s\S]*workflow_kind in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)[\s\S]*_candidate_weekly_withdrawal_reset_v1/i);
  assert.match(workflow,
    /v_action='CANCEL'[\s\S]*workflow_kind='DAILY'[\s\S]*_candidate_daily_submission_reset_v1/i);
});

test('weekly withdrawal returns to the editable contract week without deleting history', () => {
  assert.match(reset,
    /is_current=false,[\s\S]*status='REVOKED'[\s\S]*timesheet_id=null,[\s\S]*status='OPEN'/i);
  assert.match(reset,
    /timesheet_id=null[\s\S]*status='OPEN'[\s\S]*day_entries_json='\[\]'::jsonb[\s\S]*totals_json='\{\}'::jsonb/i);
  assert.doesNotMatch(reset.slice(0, reset.indexOf('create or replace function private._candidate_daily_submission_reset_v1')),
    /insert into public\.timesheets\s*\(/i);
  assert.match(reset,
    /timesheet_evidence set processing_state='SUPERSEDED'[\s\S]*CANDIDATE_SUBMISSION_WITHDRAWN_TO_CONTRACT_WEEK/i);
  assert.doesNotMatch(reset,
    /ts_financials_outbox\([\s\S]{0,300}'CANDIDATE_(?:WITHDRAWN|REJECTED)'/i);
  assert.doesNotMatch(reset, /\bdelete\s+from\b/i);
});

test('manager-refused planned weeks without a Timesheet reopen without inventing a target', () => {
  assert.match(nullTargetReset,
    /\\ir 26082026_0659_candidate_no_work_weekly_chain_v1\.sql[\s\S]*\\ir 27082026_0423_candidate_electronic_rejection_resubmission_v1\.sql/i);
  assert.match(nullTargetReset,
    /coalesce\([\s\S]*v_workflow\.target_timesheet_id[\s\S]*v_workflow\.anchor_timesheet_id[\s\S]*v_week\.timesheet_id[\s\S]*\)/i);
  assert.match(nullTargetReset,
    /if v_source_timesheet_id is not null[\s\S]*v_has_source:=true[\s\S]*elsif v_week\.timesheet_id is not null[\s\S]*CANDIDATE_WORKFLOW_TARGET_NOT_FOUND/i);
  assert.match(nullTargetReset,
    /timesheet_id=null,[\s\S]*status='OPEN'[\s\S]*day_entries_json='\[\]'::jsonb[\s\S]*totals_json='\{\}'::jsonb/i);
  assert.match(nullTargetReset,
    /case when v_has_source then 'timesheet' else 'contract_week' end/i);
  assert.doesNotMatch(nullTargetReset, /\bdelete\s+from\b/i);
  assert.match(nullTargetVerification,
    /begin;[\s\S]*'REFUSED'[\s\S]*null,null[\s\S]*_candidate_weekly_withdrawal_reset_v1[\s\S]*object_type='contract_week'[\s\S]*rollback;/i);
});

test('withdrawal helper remains private and browser-inaccessible', () => {
  assert.match(reset,
    /revoke all on function private\._candidate_weekly_withdrawal_reset_v1\([\s\S]*from public,anon,authenticated/i);
  assert.match(reset,
    /grant execute on function private\._candidate_weekly_withdrawal_reset_v1\([\s\S]*to service_role/i);
});

test('Candidate withdrawals use the configured system audit actor and never the Candidate identity', () => {
  assert.match(reset,
    /select settings_row\.candidate_app_system_actor_user_id[\s\S]*from public\.settings_defaults settings_row[\s\S]*public\.tms_users actor_row[\s\S]*btrim\(p_reason\),v_system_actor_user_id/i);
  assert.doesNotMatch(reset,
    /CANDIDATE_SUBMISSION_WITHDRAWN_TO_CONTRACT_WEEK[\s\S]{0,1200}btrim\(p_reason\),v_workflow\.candidate_id/i);
  assert.match(reset,
    /v_event='OFFICE_REJECTED'[\s\S]*where actor_row\.id=p_actor_user_id[\s\S]*else[\s\S]*candidate_app_system_actor_user_id[\s\S]*btrim\(p_reason\),v_audit_actor_user_id/i);
  assert.doesNotMatch(reset,
    /CANDIDATE_DAILY_SUBMISSION_WITHDRAWN_VERSION_ROTATED[\s\S]{0,1200}btrim\(p_reason\),p_actor_user_id/i);
});

test('Daily withdrawal preserves the booked shift while returning a clean editable Daily record', () => {
  assert.match(reset,
    /_candidate_daily_submission_reset_v1\([\s\S]*sheet_scope<>'DAILY'[\s\S]*booking_id/i);
  assert.match(reset,
    /is_current=false,[\s\S]*status='REVOKED'[\s\S]*insert into public\.timesheets/i);
  assert.match(reset,
    /scheduled_start_iso,scheduled_end_iso[\s\S]*v_current\.scheduled_start_iso,[\s\S]*v_current\.scheduled_end_iso/i);
  assert.match(reset,
    /null,null,null,null,null,null,null,'\{\}'::jsonb,'\{\}'::jsonb[\s\S]*'ELECTRONIC'/i);
  assert.match(reset,
    /insert into public\.timesheets_financials\([\s\S]*processing_status,total_hours[\s\S]*'UNASSIGNED',0/i);
  assert.doesNotMatch(reset,
    /v_new_timesheet_id[\s\S]{0,500}ts_financials_outbox/i);
  assert.match(reset,
    /CANDIDATE_DAILY_SUBMISSION_WITHDRAWN_VERSION_ROTATED[\s\S]*effective_submission_mode','ELECTRONIC'/i);
  assert.doesNotMatch(reset, /\bdelete\s+from\b/i);
});

test('Candidate cancel and Office reject both use the protected Daily reset without a contract_week', () => {
  assert.match(workflow,
    /v_action='CANCEL'[\s\S]*v_workflow\.scope='DAILY'[\s\S]*v_workflow\.workflow_kind='DAILY'[\s\S]*_candidate_daily_submission_reset_v1/i);
  assert.match(officeReject,
    /if not found and v_timesheet\.sheet_scope<>'DAILY'[\s\S]*_candidate_daily_submission_reset_v1\([\s\S]*'OFFICE_REJECTED'/i);
  assert.match(officeReject,
    /if v_week\.id is not null then[\s\S]*update public\.contract_weeks set status='OPEN'/i);
  assert.match(officeReject,
    /sheet_scope='DAILY'[\s\S]*then 'UNASSIGNED'::public\.ts_fin_processing_status_enum/i);
});

test('Daily reset is private, browser-inaccessible and has rollback-contained first-use proof', () => {
  assert.match(reset,
    /revoke all on function private\._candidate_daily_submission_reset_v1\([\s\S]*from public,anon,authenticated/i);
  assert.match(reset,
    /grant execute on function private\._candidate_daily_submission_reset_v1\([\s\S]*to service_role/i);
  assert.match(dailyVerification,
    /begin;[\s\S]*_candidate_daily_submission_reset_v1\([\s\S]*scheduled_start_iso[\s\S]*processing_status[\s\S]*rollback;/i);
  assert.match(dailyVerification,
    /exists\([\s\S]*ts_financials_outbox[\s\S]*timesheet_id=v_new_timesheet/i);
});

test('withdrawal read authority cannot replay obsolete public Candidate reads', () => {
  assert.match(finalAuthority,
    /\\ir 07082026_2108_candidate_app_read_and_missing_week_rpcs_v1\.sql[\s\S]*\\ir 07082026_2120_candidate_workflow_transition_atomic_v1\.sql/i);
  for (const currentAuthority of [
    '25082026_2024_candidate_nullif_runtime_correction_v1.sql',
    '25082026_2043_candidate_home_notification_runtime_v1.sql',
    '25082026_2255_candidate_home_actionable_timesheet_count_v1.sql',
    '26082026_1432_candidate_home_draft_counts_v1.sql',
    '26082026_1516_candidate_timesheet_card_draft_linkage_v1.sql',
    '26082026_1537_candidate_home_draft_identity_v1.sql',
    '27082026_0858_candidate_finalised_artifact_readiness_v1.sql',
    '28082026_0505_candidate_refused_card_recovery_v1.sql',
    '28082026_2002_candidate_daily_detail_projection_v1.sql',
    '29082026_0012_candidate_daily_active_window_entry_v1.sql',
    '30082026_0125_candidate_submitted_weekly_card_linkage.sql',
    '30082026_1903_candidate_expense_carrier_anchor_route_v1.sql',
    '31082026_0557_candidate_empty_expense_carrier_action_v1.sql',
    '02092026_1918_candidate_finalised_hours_primary_action_v1.sql',
    '04092026_1952_candidate_expense_history_anchor_recovery_v1.sql'
  ]) {
    assert.ok(finalAuthority.includes(`\\ir ${currentAuthority}`),
      `Final authority must replay ${currentAuthority}`);
  }
  assert.match(finalAuthority,
    /\\ir 31082026_0557_candidate_empty_expense_carrier_action_v1\.sql[\s\S]*\\ir 02092026_1918_candidate_finalised_hours_primary_action_v1\.sql/i,
    'The current submitted-hours primary action must remain the final helper owner during UPGRADE replay');
  assert.match(finalAuthority,
    /\\ir 02092026_1918_candidate_finalised_hours_primary_action_v1\.sql[\s\S]*\\ir 04092026_1952_candidate_expense_history_anchor_recovery_v1\.sql/i,
    'The current expense-history transition must remain the final workflow owner during UPGRADE replay');
  assert.match(dailyVerification,
    /search_path=""[\s\S]*draft_week\.id[\s\S]*final_signed_document_ready/i);
});

test('cancelled workflows remain history and never project as the current replacement approval', () => {
  assert.match(currentDetail,
    /w\.state not in \('CANCELLED','SUPERSEDED'\)/i);
  assert.match(currentDetail,
    /document_workflow\.state not in \('CANCELLED','SUPERSEDED'\)/i);
});
