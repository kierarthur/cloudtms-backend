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
const officeReject = fs.readFileSync(path.join(root,
  'supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql'), 'utf8');
const candidateRead = fs.readFileSync(path.join(root,
  'supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql'), 'utf8');
const dailyVerification = fs.readFileSync(path.join(root,
  'supabase/verification/27082026_1327_candidate_daily_withdrawal_reset_verification.sql'), 'utf8');

test('candidate cancellation retires approval authority and resets weekly hours atomically', () => {
  assert.match(workflow,
    /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*_candidate_manager_mail_retire_v1[\s\S]*_candidate_weekly_withdrawal_reset_v1[\s\S]*submission_withdrawal_reset/i);
  assert.match(reset,
    /scope<>'WEEKLY'[\s\S]*workflow_kind not in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)/i);
  assert.match(reset,
    /authorised_at_server is not null[\s\S]*authorised_at_utc is not null[\s\S]*paid_at_utc is not null[\s\S]*locked_by_invoice_id is not null/i);
});

test('manager-approved finalised submissions remain withdrawable until Office protection begins', () => {
  assert.match(workflow,
    /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*v_action='SUPERSEDE' and v_workflow\.state='FINALISED'[\s\S]*_candidate_weekly_withdrawal_reset_v1/i);
  assert.match(workflow,
    /v_action in \('CANCEL','SUPERSEDE'\) then\s+if v_workflow\.state in \('CANCELLED','REJECTED','SUPERSEDED'\)\s+or \(v_action='SUPERSEDE' and v_workflow\.state='FINALISED'\)/i);
  assert.match(candidateRead,
    /'AWAITING_PAPER_RETURN','RECEIVED','REFUSED','FINALISED'[\s\S]*p_candidate_status_code[\s\S]*not in \('PAID','AUTHORISED','INVOICED_NOT_PAID'\)/i);
});

test('weekly withdrawal creates a clean current version without deleting history', () => {
  assert.match(reset,
    /is_current=false,[\s\S]*status='REVOKED'[\s\S]*insert into public\.timesheets/i);
  assert.match(reset,
    /timesheet_id=v_new_timesheet_id[\s\S]*status='OPEN'[\s\S]*day_entries_json='\[\]'::jsonb[\s\S]*totals_json='\{\}'::jsonb/i);
  assert.match(reset,
    /processing_status='UNPROCESSED'[\s\S]*total_hours=0[\s\S]*expenses_pay_ex_vat=0[\s\S]*mileage_units=0/i);
  assert.match(reset,
    /'MANUAL',v_current\.line_type[\s\S]*'effective_submission_mode',v_week\.submission_mode_snapshot/i);
  assert.match(reset,
    /timesheet_evidence set processing_state='SUPERSEDED'[\s\S]*CANDIDATE_SUBMISSION_WITHDRAWN_VERSION_ROTATED/i);
  assert.doesNotMatch(reset, /\bdelete\s+from\b/i);
});

test('withdrawal helper remains private and browser-inaccessible', () => {
  assert.match(reset,
    /revoke all on function private\._candidate_weekly_withdrawal_reset_v1\([\s\S]*from public,anon,authenticated/i);
  assert.match(reset,
    /grant execute on function private\._candidate_weekly_withdrawal_reset_v1\([\s\S]*to service_role/i);
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
    /processing_status='UNASSIGNED'[\s\S]*total_hours=0[\s\S]*expenses_pay_ex_vat=0[\s\S]*mileage_units=0/i);
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
});
