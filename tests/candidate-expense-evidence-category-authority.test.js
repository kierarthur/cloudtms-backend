import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(
  new URL('../supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql', import.meta.url),
  'utf8'
);

test('categorised expense totals do not also invent an OTHER evidence requirement', () => {
  const otherRule = source.match(
    /if abs\(coalesce\(nullif\(v_expense_submission#>>'\{canonical_tsfin_snapshot,other_pay_ex_vat\}'[\s\S]*?v_required_categories:=array_append\(v_required_categories,'OTHER'\);/
  )?.[0] || '';

  assert.match(otherRule, /expenses_pay_ex_vat/);
  assert.match(otherRule, /expenses_charge_ex_vat/);
  assert.match(otherRule, /travel_pay_ex_vat/);
  assert.match(otherRule, /accommodation_pay_ex_vat/);
  assert.match(otherRule, /other_pay_ex_vat/);
  assert.match(otherRule, /\)=0\s*\) then/);
});

test('each claimed expense category still requires its own immutable evidence', () => {
  assert.match(source, /foreach v_required_category in array v_required_categories loop/);
  assert.match(source, /source_component\.expense_category=v_required_category/);
  assert.match(source, /then raise exception 'EXPENSE_EVIDENCE_REQUIRED'/);
});

