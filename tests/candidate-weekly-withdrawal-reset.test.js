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

test('candidate cancellation retires approval authority and resets weekly hours atomically', () => {
  assert.match(workflow,
    /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*_candidate_manager_mail_retire_v1[\s\S]*_candidate_weekly_withdrawal_reset_v1[\s\S]*submission_withdrawal_reset/i);
  assert.match(reset,
    /scope<>'WEEKLY'[\s\S]*workflow_kind not in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)/i);
  assert.match(reset,
    /authorised_at_server is not null[\s\S]*authorised_at_utc is not null[\s\S]*paid_at_utc is not null[\s\S]*locked_by_invoice_id is not null/i);
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
