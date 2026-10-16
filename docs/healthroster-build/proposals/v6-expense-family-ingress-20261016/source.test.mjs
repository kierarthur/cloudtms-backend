import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const read=p=>fs.readFileSync(path.join(here,p),'utf8').replace(/\r\n/g,'\n');
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
const owners=JSON.parse(read('NATIVE_OWNER_PREFIXES.json'));
test('all twelve actual current owners are explicit full functions with exactly recorded finite changes',()=>{
 assert.equal(owners.targets.length,12);
 for(const owner of owners.targets){
  const original=read(owner.reference),actual=read(owner.forward);
  assert.equal(sha(original),owner.original_complete_sha256_lf,owner.name);
  let expected=original;
  for(const e of [...owner.edits].sort((a,b)=>b.offset-a.offset)){
   assert.equal(expected.slice(e.offset,e.offset+(e.remove??'').length),e.remove??'');
   expected=expected.slice(0,e.offset)+e.text+expected.slice(e.offset+(e.remove??'').length);
  }
  assert.equal(actual,expected,owner.name);assert.equal(sha(actual),owner.forward_sha256_lf);
  assert.ok(scanManagedSql(actual).statements.length>=3);
 }
});
test('final component and submit owners preserve later authoritative native definitions',()=>{
 assert.equal(owners.targets.find(t=>t.kind==='candidate').input,'supabase/repeatable/24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql');
 assert.equal(owners.targets.find(t=>t.forward.includes('070_submit')).input,'supabase/repeatable/08092026_0918_candidate_expense_update_root_snapshot_validation_v1.sql');
});
test('all actual ingress business locks and native operation mutexes occur after the locking-only prefix',()=>{
 for(const owner of owners.targets){
  const s=read(owner.forward),at=s.indexOf('perform private.timesheet_contract_expense_famil');
  assert.ok(at>=0,owner.name);assert.ok(at<s.search(/\bfor update\b/i)||s.search(/\bfor update\b/i)<0,owner.name);
  if(['office','candidate','whole','resubmit'].includes(owner.kind))assert.ok(at<s.search(/perform (?:pg_catalog\.)?pg_advisory_xact_lock/),owner.name);
 }
});
test('native request hash and immutable replay branch remain before original mutable generation checks',()=>{
 for(const file of ['030_office_reject','040_component','050_whole','100_paper_begin','110_category_resubmit']){
  const s=read('forward/'+file+'.sqlinc');
  assert.ok(s.indexOf('v_request_sha')<s.indexOf("v_operation.state='COMMITTED'"));
  assert.ok(s.indexOf("v_operation.state='COMMITTED'")<s.search(/if v_(?:workflow|source|owner)\.generation/),file);
 }
});
test('kernel locks actual candidates/controls/families in global order and never makes a currentness/authentication certificate',()=>{
 const s=read('forward/001_family_entry.sqlinc');
 assert.ok(s.indexOf('from public.candidates c')<s.indexOf('from private.bpay_next_worker_control c'));
 assert.ok(s.indexOf('from private.bpay_next_worker_control c')<s.indexOf('from private.timesheet_contract_current_placements p'));
 assert.ok(s.indexOf('from private.timesheet_contract_current_placements p')<s.indexOf('from public.timesheets t'));
 for(const order of ['order by c.id for update','order by c.candidate_id for update','order by p.family_root_id,p.member_key for update','order by t.timesheet_id for update'])assert.ok(s.includes(order),order);
 assert.match(s,/after_identity is distinct from before_identity/);
 assert.doesNotMatch(s,/timesheet_contract_current_placement_v1\(|unchanged_facts|set_config|insert into|update public\.|delete from|grant execute/i);
 assert.match(s,/candidate_expense_components e on e.workflow_id=w.id/);
 assert.match(s,/p_workflow_ids\)>100/);
});
test('watchdog freezes same native bounded deadline/id selection and locks the full candidate set before iteration',()=>{
 const s=read('forward/120_recover.sqlinc');
 assert.match(s,/row_number\(\) over\(order by update_row.expires_at_utc,update_row.update_id\) ordinal/);
 assert.match(s,/limit p_limit/);assert.match(s,/p_limit not between 1 and 100/);
 assert.ok(s.indexOf('timesheet_contract_expense_families_enter_v1')<s.indexOf('for v_target in'));
 assert.match(s,/with ordinality rows\(item,ordinal\) order by ordinal/);
});
test('manager read-ahead requires actual pending token ownership and original native under-lock recheck remains intact',()=>{
 const s=read('forward/090_manager_hold.sqlinc');
 assert.equal((s.match(/token_hash=decode\(lower\(p_approval_token_hash_hex\),'hex'\)/g)??[]).length,2);
 assert.ok(s.indexOf('if not exists')<s.indexOf('timesheet_contract_expense_family_enter_v1'));
 assert.match(s,/request.id=v_update.approval_request_id/);
});
