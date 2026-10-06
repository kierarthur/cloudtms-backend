import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = path => readFileSync(new URL('../' + path, import.meta.url), 'utf8').replaceAll('\r\n', '\n');
const predecessor = read('supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql');
const successor = read('supabase/repeatable/06102026_1214_self_bill_candidate_expense_separation_v1.sql');
const verifier = read('supabase/verification/03092026_1642_contract_settings_effective_authority_verification.sql');
const finalGuard = read('supabase/repeatable/04092026_2232_candidate_import_expense_carrier_finalisation_v1.sql');
const core = source => source.match(/create or replace function private\._contract_settings_effective_core_v1\([\s\S]*?\$function\$;/)?.[0];

test('self-billing mandates separation using the actual inherited self-bill setting', () => {
  assert.match(successor, /v_self_bill:=case when v_override then coalesce\(v_contract\.self_bill,false\)\s+else coalesce\(v_client\.self_bill_no_invoices_sent,false\) end/);
  assert.match(successor, /'self_bill',v_self_bill/);
  assert.match(successor, /'candidate_expenses_require_separate_timesheet',case when v_import_authoritative then true\s+when v_self_bill then true/);
  assert.match(successor, /when v_import_authoritative then 'IMPORT_MANDATORY'\s+when v_self_bill then 'SELF_BILL_MANDATORY'/);
});

test('the complete replacement changes only expense policy and deduplicates identical self-bill resolution', () => {
  let expected = core(predecessor);
  expected = expected.replace('  v_import_authoritative boolean := false;', '  v_import_authoritative boolean := false;\n  v_self_bill boolean := false;');
  expected = expected.replace("  select coalesce(jsonb_agg(to_jsonb(h.date_value) order by h.date_value),'[]'::jsonb)",
    "  -- Self-billing controls expense separation, not Candidate hour-entry authority.\n  v_self_bill:=case when v_override then coalesce(v_contract.self_bill,false)\n    else coalesce(v_client.self_bill_no_invoices_sent,false) end;\n\n  select coalesce(jsonb_agg(to_jsonb(h.date_value) order by h.date_value),'[]'::jsonb)");
  expected = expected.replace("'self_bill',case when v_override then coalesce(v_contract.self_bill,false)\n      else coalesce(v_client.self_bill_no_invoices_sent,false) end,", "'self_bill',v_self_bill,");
  expected = expected.replace("'candidate_expenses_require_separate_timesheet',case when v_import_authoritative then true\n      else", "'candidate_expenses_require_separate_timesheet',case when v_import_authoritative then true\n      when v_self_bill then true\n      else");
  expected = expected.replace("when v_import_authoritative then 'IMPORT_MANDATORY'\n      when p_contract_id", "when v_import_authoritative then 'IMPORT_MANDATORY'\n      when v_self_bill then 'SELF_BILL_MANDATORY'\n      when p_contract_id");
  assert.equal(core(successor), expected);
  assert.equal((successor.match(/create or replace function/g) || []).length, 1);
  assert.doesNotMatch(successor, /(?:update|insert into|delete from) public\.(?:candidate_submission|candidate_expense|timesheets|contract_weeks|pay_)/i);
});

test('ordinary hour entry, frozen snapshots and service-only security stay intact', () => {
  assert.match(successor, /v_is_nhsp or \(v_autoprocess_hr and v_no_timesheet_required\)/);
  assert.match(successor, /'candidate_hours_view_only',v_import_authoritative/);
  assert.match(successor, /return private\._timesheet_settings_authority_frozen_v1\(p_timesheet_id\)/);
  assert.match(successor, /revoke all on function private\._contract_settings_effective_core_v1\(uuid,uuid,date,text,uuid\)\s+from public,anon,authenticated,service_role/);
  assert.match(successor, /notify pgrst, 'reload schema'/);
});

test('the registered runtime verifier checks inheritance, override, daily and actual final-state admission', () => {
  for (const code of [
    'ORDINARY_NON_SELF_BILL_COMBINED_POLICY_CHANGED',
    'ORDINARY_SELF_BILL_CLIENT_EXPENSE_SEPARATION_INVALID',
    'ORDINARY_SELF_BILL_DAILY_EXPENSE_SEPARATION_INVALID',
    'SELF_BILL_CONTRACT_FALSE_OVERRIDE_IGNORED',
    'SELF_BILL_CONTRACT_TRUE_OVERRIDE_NOT_ENFORCED',
    'SELF_BILL_COMBINED_SUBMISSION_ADMITTED',
    'SELF_BILL_ORDINARY_HOUR_ENTRY_BLOCKED',
    'SELF_BILL_SEPARATE_EXPENSE_ENTRY_BLOCKED',
  ]) assert.ok(verifier.includes(code), code);
  assert.match(verifier, /exception when sqlstate '22023'/);
  assert.match(verifier, /sqlerrm is distinct from 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS'/);
  assert.match(finalGuard, /v_policy:=private\._candidate_policy_resolve_v1/);
  assert.match(finalGuard, /'expenses_require_separate_timesheet'[\s\S]*?HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS/);
});
