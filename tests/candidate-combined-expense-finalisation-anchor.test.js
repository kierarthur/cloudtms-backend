import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const finalisation = readFileSync(
  new URL('../supabase/repeatable/28082026_1925_candidate_daily_receipt_finalisation_v1.sql', import.meta.url),
  'utf8'
).replace(/\r\n/g, '\n');
const expenseApply = readFileSync(
  new URL('../supabase/repeatable/05092026_0035_candidate_expense_carrier_approved_projection_v1.sql', import.meta.url),
  'utf8'
).replace(/\r\n/g, '\n');

test('first combined weekly finalisation binds its worked Timesheet before same-record expense apply', () => {
  const placement = finalisation.indexOf("'placement','SAME_RECORD'");
  const binding = finalisation.indexOf(
    "anchor_timesheet_id=case when v_workflow.workflow_kind='CONTRACT_COMBINED'"
  );
  const expenseCall = finalisation.indexOf('v_result:=public.timesheet_expense_apply_atomic_v1(', binding);

  assert.ok(placement >= 0, 'same-record placement must remain explicit');
  assert.ok(binding > placement, 'the worked Timesheet anchor must be bound after placement is resolved');
  assert.ok(expenseCall > binding, 'the anchor must be stored before expense apply reads the workflow');
  assert.match(
    finalisation.slice(binding, expenseCall),
    /then coalesce\(anchor_timesheet_id,v_hours_timesheet_id\)[\s\S]*?contract_week_id=nullif\(v_placement->>'target_contract_week_id',''\)::uuid[\s\S]*?target_timesheet_id=nullif\(v_placement->>'target_timesheet_id',''\)::uuid/
  );
});

test('expense apply still distinguishes a same-record combined claim from a genuine separate carrier', () => {
  assert.match(
    expenseApply,
    /v_is_separate_carrier:=v_workflow\.workflow_kind='CONTRACT_EXPENSE'[\s\S]*?or v_workflow\.target_timesheet_id is distinct from v_workflow\.anchor_timesheet_id;/
  );
  assert.match(
    expenseApply,
    /case when v_is_separate_carrier then jsonb_build_object\([\s\S]*?'line_type',v_expense_line_type[\s\S]*?else v_electronic_patch end/
  );
});
