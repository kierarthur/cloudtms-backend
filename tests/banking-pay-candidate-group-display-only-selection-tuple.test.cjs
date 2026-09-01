const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');

const root=path.resolve(__dirname,'..');
const replacement=fs.readFileSync(path.join(root,
  'supabase/repeatable/01092026_1058_banking_pay_candidate_current_payable_groups_v2.sql'),'utf8');
const worker=fs.readFileSync(path.join(root,'broker/src/banking-pay-modal-v2.js'),'utf8');

test('Timesheet and overpayment pages require current selectable group facts',()=>{
  assert.match(replacement,/WHERE r\.presentation_group_kind='ROW' OR f\.group_kind IS NOT NULL/);
  assert.match(replacement,/AND \(p_group_kind='ROW' OR \(g\.group_kind=p_group_kind AND g\.group_key=p_group_key\)\)/);
  assert.match(replacement,/'selection_group_display_amount',CASE WHEN r\.group_kind IS NOT NULL THEN to_char\(r\.full_display_amount/);
  assert.match(replacement,/'selection_group_selected_display_amount',CASE WHEN r\.group_kind IS NOT NULL THEN to_char\(r\.selected_display_amount/);
});

test('the Worker accepts a null selection tuple only for a retained single-row financial presentation',()=>{
  assert.match(worker,/if \(row\.selection_group_kind===null\) return row\.presentation_group_kind==='ROW'/);
  assert.match(worker,/row\.selection_group_selected_count===0&&row\.selection_group_state===null/);
  assert.match(worker,/row\.selection_group_display_amount===null&&row\.selection_group_selected_display_amount===null/);
  assert.match(worker,/row\.presentation_group_kind===row\.selection_group_kind/);
});

test('the correction remains a bounded read projection and preserves its security envelope',()=>{
  assert.doesNotMatch(replacement,/\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\./i);
  assert.match(replacement,/SET search_path TO '' SET statement_timeout TO '3s' SET lock_timeout TO '1s'/);
  assert.doesNotMatch(replacement,/pay_workbench_prepare_draft_scope_seed/);
  assert.match(replacement,/REVOKE ALL ON FUNCTION public\.pay_workbench_session_get_candidate_ready_group_page_v1[^;]+FROM PUBLIC,anon,authenticated/);
  assert.match(replacement,/GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_get_candidate_ready_group_page_v1[^;]+TO service_role/);
});
