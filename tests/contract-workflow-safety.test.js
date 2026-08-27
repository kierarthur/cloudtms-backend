import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const cloneSql = await readFile(new URL('../supabase/repeatable/22082026_1706_daily_validation_compatibility_authorities_v1.sql', import.meta.url), 'utf8');

const section = (startMarker, endMarker) => {
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `missing ${startMarker}`);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.notEqual(end, -1, `missing ${endMarker}`);
  return source.slice(start, end);
};

test('started Contract updates compare actual protected values and do not reject unrelated safe saves', () => {
  const update = section('async function handleContractsUpdate', 'async function handleContractsReplace');
  for (const field of [
    'candidate_id', 'client_id', 'role', 'band', 'pay_method_snapshot',
    'weekly_timesheet_source', 'default_submission_mode', 'overrideclientsettings',
    'is_nhsp', 'autoprocess_hr', 'requires_hr', 'no_timesheet_required',
    'self_bill', 'daily_calc_of_invoices', 'group_nightsat_sunbh', 'auto_invoice'
  ]) assert.match(update, new RegExp(`['\"]${field}['\"]`), `missing protected ${field}`);
  assert.match(update, /compareString/);
  assert.match(update, /compareTri/);
  assert.match(update, /changedCore\.length/);
  assert.match(update, /Core contract details cannot be changed after the contract has started/);
});

test('Contract replacement uses the same lifecycle locks and preserves week-ending authority once weeks exist', () => {
  const replace = section('async function handleContractsReplace', 'async function handleContractsDelete');
  assert.match(replace, /compareString/);
  assert.match(replace, /compareTri/);
  assert.match(replace, /changedCore\.length/);
  assert.match(replace, /Week-ending day cannot be changed after contract weeks have been created/);
});

test('duplicate Contract supports assigned and vacant copies with optimistic locking and fatal week generation', () => {
  const duplicate = section('async function handleContractsDuplicate', 'async function generateContractWeeksInternal');
  assert.match(duplicate, /expected_source_updated_at/);
  assert.match(duplicate, /source Contract changed while this window was open/i);
  assert.match(duplicate, /assignments/);
  assert.match(duplicate, /candidate_manager_approval_policy_json/);
  assert.match(duplicate, /candidate_paper_submission_enabled_override/);
  assert.match(duplicate, /await generateContractWeeksInternal/);
  assert.match(duplicate, /duplicates/);
  assert.match(duplicate, /rollbackDuplicateContracts/);
  assert.match(duplicate, /duplicateFailureResponse/);
  assert.match(duplicate, /cleanup_failure_count/);
  assert.match(duplicate, /method: 'DELETE'/);
});

test('finish and extend is optimistic and delegates to the atomic database authority', () => {
  const start = source.indexOf('async function handleContractsCloneAndExtend');
  assert.notEqual(start, -1);
  const clone = source.slice(start, start + 18_000);
  assert.match(clone, /expected_source_updated_at/);
  assert.match(clone, /source Contract changed while this extension was being prepared/i);
  assert.match(clone, /contracts_clone_and_extend_atomic/);
});

test('atomic successor preserves independent Contract policies', () => {
  assert.match(cloneSql, /candidate_paper_submission_enabled_override/);
  assert.match(cloneSql, /candidate_manager_approval_policy_json/);
  assert.match(cloneSql, /v_cur\.candidate_paper_submission_enabled_override/);
  assert.match(cloneSql, /v_cur\.candidate_manager_approval_policy_json/);
});

test('printed-timesheet authority remains independently editable after start with optimistic locking', () => {
  const policy = section('async function handleContractPrintedTimesheetPolicyUpdate', 'async function handleUpdateClient');
  assert.match(policy, /currentOverride === requestedOverride/);
  assert.match(policy, /unchanged: true/);
  assert.match(policy, /expected_contract_updated_at/);
  assert.match(policy, /updated_at=eq\./);
  assert.doesNotMatch(policy, /CONTRACT_STARTED_FIELD_LOCKED/);
});

test('override-off clears nullable governed values and override-on seeds the complete Client snapshot', async () => {
  const sql = await readFile(new URL('../supabase/repeatable/27082026_1946_contract_override_inheritance_v2.sql', import.meta.url), 'utf8');
  assert.match(sql, /if new\.overrideclientsettings = false then/);
  for (const field of [
    'no_timesheet_required', 'daily_calc_of_invoices', 'group_nightsat_sunbh',
    'is_nhsp', 'autoprocess_hr', 'requires_hr', 'hr_attach_to_invoice',
    'ts_attach_to_invoice', 'reference_number_required_to_issue_invoice',
    'send_manual_invoices_to_different_email', 'manual_invoices_alt_email_address',
    'default_submission_mode', 'timesheet_break_entry_mode'
  ]) assert.match(sql, new RegExp(`new\\.${field}\\s*:=\\s*null`), `OFF must clear ${field}`);
  for (const clientField of [
    'default_submission_mode', 'timesheet_break_entry_mode', 'auto_invoice_default',
    'pay_reference_required', 'invoice_reference_required', 'self_bill_no_invoices_sent'
  ]) assert.match(sql, new RegExp(`cs\\.${clientField}`), `ON must seed ${clientField}`);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)/i);
});
