import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const source = fs.readFileSync(new URL(
  '../supabase/repeatable/05092026_0941_candidate_protected_additional_expense_action_v1.sql',
  import.meta.url,
), 'utf8');
const verification = fs.readFileSync(new URL(
  '../supabase/verification/02092026_1920_candidate_finalised_hours_primary_action_verification.sql',
  import.meta.url,
), 'utf8');
const protectedVerification = fs.readFileSync(new URL(
  '../supabase/verification/05092026_0942_candidate_protected_additional_expense_action_verification.sql',
  import.meta.url,
), 'utf8');
const primaryActionSource = source.slice(
  source.indexOf('create or replace function private._candidate_timesheet_primary_action_v1'),
  source.indexOf('create or replace function private._candidate_timesheet_action_contract_v1'),
);

test('manager-approved finalised hours open a separate expense claim without waiting for agency authorisation', () => {
  assert.match(source, /v_has_finalised_hours[\s\S]*state'='FINALISED'[\s\S]*CONTRACT_HOURS'[\s\S]*CONTRACT_COMBINED'[\s\S]*DAILY'/i);
  assert.doesNotMatch(source, /v_hours_office_protected|hours_financial\.authorised_at_utc/i);
  assert.match(source, /if v_has_finalised_hours[\s\S]*can_edit_expenses'[\s\S]*'ADD_EXPENSES'[\s\S]*return null/i);
  assert.match(source, /create or replace function private\._candidate_timesheet_action_contract_v1[\s\S]*jsonb_array_length\(coalesce\(p_workflows,'\[\]'::jsonb\)\)=0[\s\S]*ENTER_TIMESHEET/i);
  assert.match(verification, /PENDING_AUTH[\s\S]*CONTRACT_COMBINED[\s\S]*FINALISED[\s\S]*ADD_EXPENSES/i);
  assert.match(verification, /AWAITING_MANAGER_APPROVAL[\s\S]*CONTRACT_COMBINED[\s\S]*CONTINUE_TIMESHEET/i);
  assert.match(protectedVerification, /PENDING_AUTH[\s\S]*ADD_EXPENSES[\s\S]*AUTHORISED[\s\S]*INVOICED_NOT_PAID[\s\S]*PAID/i);
  assert.match(protectedVerification, /CONTRACT_HOURS[\s\S]*PENDING_AUTH[\s\S]*ADD_EXPENSES/i);
  assert.match(protectedVerification, /AWAITING_MANAGER_APPROVAL[\s\S]*CONTINUE_TIMESHEET[\s\S]*parallel expense claim/i);
  assert.match(verification, /CONTRACT_EXPENSE[\s\S]*FINALISED[\s\S]*ADD_EXPENSES/i);
  assert.doesNotMatch(source, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('a manager-approved finalised expense stops blocking the next claim', () => {
  assert.doesNotMatch(primaryActionSource, /'AWAITING_PAPER_RETURN','RECEIVED','REFUSED','FINALISED'/i);
  assert.match(source, /\('PAID','AUTHORISED','INVOICED_NOT_PAID'\)[\s\S]*can_edit_expenses/i);
  assert.match(verification, /CONTRACT_EXPENSE'[\s\S]*FINALISED[\s\S]*ADD_EXPENSES/i);
  assert.match(protectedVerification, /Manager-approved expense still blocked the next claim/i);
});
