import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
const read=p=>fs.readFileSync(new URL(p,import.meta.url),'utf8').replaceAll('\r\n','\n');
test('every native monetary, event and permission body survives exact finite reconstruction',()=>{
 for(const item of JSON.parse(read('./FINITE_REPLACEMENTS.json'))){let s=read('./'+item.reference);
  for(const e of item.edits){assert.equal(s.split(e.before).length-1,1);s=s.replace(e.before,e.after);}assert.equal(s,read('./'+item.forward));
 }
});
test('pending is actual native ownership and exact same component/member, never a guessed target',()=>{
 const s=read('./forward/024_claim_pending.sqlinc');
 for(const owner of ['timesheet_contract_native_pending_for_v1','timesheet_contract_native_pending_week_v1','timesheet_contract_native_pending_workflow_v1'])assert.ok(s.includes(owner));
 assert.match(s,/e\.workflow_id is distinct from w\.id or e\.owning_timesheet_id is distinct from p_stored_timesheet_id/);
 assert.match(s,/e\.expense_component_id=any\(m\.expense_component_ids\)/);assert.match(s,/w\.id=any\(m\.workflow_ids\)/);
 assert.match(s,/a\.family_root_id=m\.family_root_id and a\.member_key=m\.member_key/);
 assert.match(s,/w\.id=\(pending->>'workflow_id'\)::uuid and w\.workflow_kind='CONTRACT_COMBINED'/);
 assert.doesNotMatch(s,/exception when|to_regprocedure|insert into|update public|delete from|max\(version/i);
});
test('known pending is handled before every UUID resolver legacy fallback',()=>{
 for(const file of ['025_claim_placement.sqlinc','026_claim_current.sqlinc','027_claim_owned.sqlinc']){
  const s=read('./forward/'+file);const before=s.indexOf('timesheet_contract_claim_pending_v1');
  assert.ok(before>0);assert.ok(before<s.indexOf('select a.family_root_id')||before<s.indexOf('v_result:='));
  assert.match(s.slice(before,before+180),/return null/);
 }
});
test('native allocation and original rotation both retain full H103 and immutable member guards',()=>{
 const s=read('./forward/025_claim_placement.sqlinc');
 for(const branch of ['rotation_binding_v1(to_jsonb(p),false)','allocation_binding_v1(to_jsonb(p),false)','allocation_binding_v1(to_jsonb(p),true)'])assert.ok(s.includes(branch));
 for(const k of ['m.to_timesheet_id=t.timesheet_id and m.to_version=t.version','w.id=any(m.workflow_ids)','p_expense_component_id=any(m.expense_component_ids)',
  'private.timesheet_contract_current_placement_v1(t.timesheet_id)=jsonb_build_object','o.placement_receipt_json'])assert.ok(s.includes(k));
 assert.doesNotMatch(s,/update|insert into|_id.*max\(/i);
});
test('component lock uses common Candidate/Daily kernel before the row and pending before current finance',()=>{
 const s=read('./forward/028_claim_lock.sqlinc');
 assert.ok(s.indexOf('timesheet_contract_expense_family_enter_v1')<s.indexOf('for update;'));
 assert.ok(s.indexOf('timesheet_contract_claim_pending_v1')<s.indexOf('perform 1 from public.timesheets timesheet'));
 assert.ok(s.includes('v_component.workflow_id is distinct from v_initial_workflow_id'));
 assert.ok(s.includes('v_component.owning_timesheet_id is distinct from v_initial_owner_id'));
 assert.ok(s.includes('is distinct from v_timesheet_id'));
});
test('pending sync preserves real owner/protection while original native cancellation lifecycle remains',()=>{
 const s=read('./forward/030_claim_sync.sqlinc');
 assert.match(s,/v_current_owner:=case when v_pending is not null then null else/);
 assert.match(s,/if v_pending is not null then\s+v_current_owner:=v_component\.owning_timesheet_id;\s+v_fin:=null;v_timesheet:=null;\s+v_agency_state:=v_component\.agency_authorisation_state;/);
 assert.match(s,/when v_workflow\.state='CANCELLED' then 'CANCELLED'/);
 assert.match(s,/if v_component\.lifecycle_state in \([\s\S]*?'WITHDRAWN','CANCELLED','SUPERSEDED'/);
 assert.ok(s.indexOf('timesheet_contract_expense_family_enter_v1')<s.indexOf('select * into strict v_workflow'));
 assert.match(s,/for v_value[\s\S]*?loop\s+v_current_owner:=v_workflow_owner;\s+v_agency_state:=v_base_agency;/);
 const ref=read('./reference/030_claim_sync.sqlinc');const marker='    if v_component.expense_component_id is null then';
 assert.equal(s.slice(s.indexOf(marker)),ref.slice(ref.indexOf(marker)));
});
