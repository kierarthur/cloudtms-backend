const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');

const root=path.resolve(__dirname,'..');
const replacement=fs.readFileSync(path.join(root,
  'supabase/repeatable/31082026_1905_banking_pay_candidate_group_display_only_selection_tuple_v1.sql'),'utf8');
const worker=fs.readFileSync(path.join(root,'broker/src/banking-pay-modal-v2.js'),'utf8');

test('display-only detail rows use the existing null and zero selection tuple',()=>{
  assert.match(replacement,/'selection_group_kind',r\.group_kind,'selection_group_key',r\.group_key/);
  assert.match(replacement,/'selection_group_member_count',CASE WHEN r\.group_kind IS NULL THEN 0 ELSE COALESCE\(f\.member_count,0\) END/);
  assert.match(replacement,/'selection_group_selected_count',CASE WHEN r\.group_kind IS NULL THEN 0 ELSE COALESCE\(f\.selected_count,0\) END/);
  assert.match(replacement,/'selection_group_display_amount',CASE WHEN r\.group_kind IS NOT NULL THEN to_char\(f\.full_display_amount/);
  assert.match(replacement,/'selection_group_selected_display_amount',CASE WHEN r\.group_kind IS NOT NULL THEN to_char\(f\.selected_display_amount/);
  assert.match(replacement,/'selection_group_state',CASE WHEN r\.group_kind IS NULL THEN NULL/);
});

test('the Worker continues to reject every contradictory selection tuple',()=>{
  assert.match(worker,/if \(row\.selection_group_kind===null\) return row\.selection_group_key===null&&row\.selection_group_member_count===0/);
  assert.match(worker,/row\.selection_group_selected_count===0&&row\.selection_group_state===null/);
  assert.match(worker,/row\.selection_group_display_amount===null&&row\.selection_group_selected_display_amount===null/);
});

test('the correction remains a bounded read projection and preserves its security envelope',()=>{
  assert.doesNotMatch(replacement,/\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\./i);
  assert.match(replacement,/SET search_path TO '' SET statement_timeout TO '3s' SET lock_timeout TO '1s'/);
  assert.match(replacement,/REVOKE ALL ON FUNCTION public\.pay_workbench_session_get_candidate_ready_group_page_v1[^;]+FROM PUBLIC,anon,authenticated/);
  assert.match(replacement,/GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_get_candidate_ready_group_page_v1[^;]+TO service_role/);
});
