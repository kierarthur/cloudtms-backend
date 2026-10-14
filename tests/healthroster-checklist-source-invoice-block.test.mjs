import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {functionEvents,sqlTokens} from '../docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {decodeChecklistHold,projectChecklistItem,projectChecklistRow} from '../broker/src/weekly-source/healthroster-checklist-source.mjs';
import {decodeChecklistRead} from '../broker/src/weekly-source/checklist-read.mjs';
const packet=new URL('../docs/healthroster-build/proposals/checklist-native-invoice-block-20261014/',import.meta.url);
const leaf=new URL('../supabase/repeatable/healthroster_wp_u3_source_proposals/',import.meta.url);
const read=p=>fs.readFileSync(new URL(p,packet),'utf8'),sql=p=>fs.readFileSync(new URL(p,leaf),'utf8');
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,H='a'.repeat(64);
const scope={source_group_id:id(1),source_cycle_id:id(2),client_id:id(3),week_ending:'2026-10-11'};
const actor={actor_user_id:id(4),permission_view:'ADMIN',active:true},pay={...actor,permission_view:'PAY_READONLY'};
const hold=()=>({holds_pay:null,holds_pay_scope:null,withheld_amount_pence:null,holds_invoice:true,pending_decision:null,
  authority:{owner:'SOURCE_INVOICE_BLOCK',hash:H,withholding_known:false}});
function item(){return {item_id:'native-invoice:'+id(8),item_kind:'NATIVE_INVOICE',scope,route:'SELF_BILL',worker:'Actual Worker',
  work_date:'2026-10-07',sheet_scope:'WEEKLY',reason_code:'VAT_FIT',status:{text:'Holds invoice',tone:'danger',subtext:'Native invoice block',subtone:'muted'},
  hold:hold(),subject:'INVOICE_ONLY',permitted_pay_subject:false,action:null,counted_timesheet_id:null,correction_binding:null,
  source_information_witness:null,submission_read:null,native_source_witness:null,
  native_invoice_witness:{owner:'private.weekly_source_invoice_unreleased_vat_fit_movements_v1',actor_user_id:actor.actor_user_id,scope,
    final_revision_id:id(6),client_manifest_id:id(7),billing_movement_id:id(8),upload_row_id:id(9),work_event_id:id(10),
    manifest_hash:H,movement_economic_hash:H,read_proof_hash:H}};}
const copy=x=>structuredClone(x);
const request={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
const envelope=x=>({ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command:request.command,actor_user_id:actor.actor_user_id,permission_view:'ADMIN',
  binding:H,read_revision:H,rows:[],items:[x],has_more:false,next_cursor:null,refresh_required:false,anchor:null});

test('same native VAT_FIT admission predicate is extracted once with complete exact wrapper and reverse',()=>{
  const p=JSON.parse(read('NATIVE_BLOCKER_REVERSE.json')),before=fs.readFileSync(new URL('../checklist-native-invoice-20261014/input/public_weekly_source_invoice_admit_atomic_v1.sqlinc',packet),'utf8');
  const forward=sql('wp_u3_invoice_block_native.forward.sqlinc'),reverse=sql('wp_u3_invoice_block_native.reverse.sqlinc');
  assert.equal(sha(before),p.before_sha256);assert.equal(sha(forward),p.forward_sha256);assert.equal(sha(reverse),p.reverse_sha256);
  const defs=new Map(functionEvents(forward).events.filter(e=>e.definition).map(e=>[e.name,e.definition]));
  const wrapper=defs.get('public.weekly_source_invoice_admit_atomic_v1');
  assert.equal(wrapper,before.replace(p.hunk.before,p.hunk.after));assert.equal(wrapper.replace(p.hunk.after,p.hunk.before),before);
  assert.equal((wrapper.match(/WEEKLY_SOURCE_INVOICE_VAT_FIT_HELD/g)||[]).length,1);
  assert.equal((forward.match(/m\.source_facts_json->>'invoice_hold'='VAT_FIT'/g)||[]).length,1);
  const current=execFileSync('git',['cat-file','blob',p.source_commit+':'+p.source_body],{maxBuffer:3e6}).toString().replaceAll('\r\n','\n');
  assert.equal(functionEvents(current).events.find(e=>e.name==='public.weekly_source_invoice_admit_atomic_v1').definition,before);
  assert.equal(scanManagedSql(forward).statements.length,4);assert.equal(scanManagedSql(reverse).statements.length,2);
  assert.ok(reverse.startsWith(before));assert.match(reverse,/drop function private\.weekly_source_invoice_unreleased_vat_fit_movements_v1\(uuid\);/);
  assert.match(forward,/revoke all on function private\.weekly_source_invoice_unreleased_vat_fit_movements_v1\(uuid\) from public,anon,authenticated,service_role/);
});
test('positive block reader is pure and delegates native predicate, with no amount or decision classifier',()=>{
  const s=sql('wp_u3_source_invoice_block.sqlinc');assert.equal(scanManagedSql(s).statements.length,3);
  const e=functionEvents(s).events.find(e=>e.definition),words=sqlTokens(e.body).filter(t=>t.kind==='word').map(t=>t.value);
  assert.deepEqual(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','round','sum'].includes(w)),[]);
  assert.doesNotMatch(s,/for (?:update|share)|invoice_line_amounts|pending_decision',true|withheld_amount_pence','0'/);
  assert.match(s,/from private\.weekly_source_invoice_unreleased_vat_fit_movements_v1\(manifest\.id\)/);
  assert.match(s,/'holds_invoice',true,'pending_decision',null/);
  assert.match(s,/read_proof_hash',hash/);
});
test('actor, dated scope and financial permission precede native invoice census',()=>{
  const s=sql('wp_u3_source_invoice_block.sqlinc');
  assert.ok(s.indexOf("lower(btrim(coalesce(u.role,'')))='admin'")<s.indexOf('for manifest in'));
  assert.ok(s.indexOf("'VIEW_SOURCE_PROGRESS'")<s.indexOf('for manifest in'));
  assert.match(s,/week_ending'\)::date between m\.valid_from and coalesce\(m\.valid_to/);
  assert.match(s,/fr\.authority_scope_kind='CYCLE' and fr\.report_scope_id is null/);
  assert.match(s,/c\.final_revision_id=revision\.id and c\.completion_kind='FINAL_SOURCE' and c\.state='CURRENT'/);
  assert.match(s,/WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_COMPLETION_UNAVAILABLE/);
});
test('actual export ledger persists without CURRENT publication and unproved native identity refuses rather than suppresses',()=>{
  const s=sql('wp_u3_source_invoice_block.sqlinc');assert.doesNotMatch(s,/projection_publications|current_complete_upload_id|known.*continue|line_amounts|limit /i);
  assert.match(s,/mm\.movement_hash is distinct from m\.movement_economic_hash/);
  assert.match(s,/WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_ITEM_IDENTITY_UNAVAILABLE/);
  assert.match(s,/WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_PLACEMENT_CONFLICT/);
  assert.match(s,/facts:=facts\|\|jsonb_build_array\(fact\);[\s\S]*if source_row\.work_date not between/);
});
test('whole producer joins same invoice facts before paging, without pay exposure, fake row admission or process shortcut',()=>{
  const s=sql('wp_u3_native_checklist.sqlinc'),begin=s.indexOf("if route='SELF_BILL' and view='ADMIN' then"),end=s.indexOf("if route='AGENCY_INVOICE' then",begin),arm=s.slice(begin,end);
  assert.ok(begin>0);assert.match(arm,/weekly_source_checklist_source_invoice_block_items_v1\(row_scope,actor\.id\)/);
  assert.match(arm,/financial_facts:=financial_facts\|\|jsonb_build_array\(invoice_blocks->'native_facts'\)/);
  assert.match(arm,/'action',null,'counted_timesheet_id',null/);assert.doesNotMatch(arm,/actual_count:=|pay_count:=|pay_complete:=|pending_decision.*true/);
  assert.match(arm,/completed:=false/);assert.ok(begin<s.indexOf("'WEEKLY_SOURCE_CHECKLIST_READ_REVISION_V1'"));
  assert.ok(begin<s.indexOf('weekly_source_checklist_page_v1('));
});
test('whole native item transport preserves real invoice block and unknown pay/decision without a fabricated action',()=>{
  const native=item(),decoded=decodeChecklistRead(envelope(native),request,{id:actor.actor_user_id,payView:false});
  assert.deepEqual(decoded.items[0].hold,hold());assert.equal(decoded.items[0].status.text,'Holds invoice');assert.equal(decoded.items[0].action,null);
  assert.equal(decoded.items[0].counted_timesheet_id,null);
});
test('reduced pay boundary requires invoice population filtered before page',()=>{
  assert.equal(projectChecklistItem(item(),pay),null);
  const frame=envelope(item());frame.permission_view='PAY_READONLY';
  assert.throws(()=>decodeChecklistRead(frame,request,{id:actor.actor_user_id,payView:true}),/NATIVE_FILTER_NOT_APPLIED/);
});
test('invoice block cannot be reused as a pay amount, known zero, cleared invoice or pending Office decision',()=>{
  for(const change of [{holds_pay:true,holds_pay_scope:'SHIFT',withheld_amount_pence:'125',authority:{owner:'SOURCE_INVOICE_BLOCK',hash:H,withholding_known:true}},
    {holds_pay:false,withheld_amount_pence:'0',authority:{owner:'SOURCE_INVOICE_BLOCK',hash:H,withholding_known:true}},
    {holds_invoice:false},{pending_decision:true},{pending_decision:false},{holds_pay_scope:'TIMESHEET'}]) {
    assert.throws(()=>decodeChecklistHold({...hold(),...change}));
  }
});
test('all new witness hashes reject arrays and nonstrings at the whole item boundary',()=>{
  for(const key of ['manifest_hash','movement_economic_hash','read_proof_hash'])for(const value of [[H],[],null,{},17,true]) {
    const x=item();x.native_invoice_witness[key]=value;assert.throws(()=>projectChecklistItem(x,actor));
  }
  const x=item();x.hold.authority.hash=[H];assert.throws(()=>projectChecklistItem(x,actor));
});
test('exact actor/scope/movement evidence and required real upload identity are bound',()=>{
  for(const change of [x=>x.native_invoice_witness.actor_user_id=id(50),x=>x.native_invoice_witness.scope={...scope,client_id:id(50)},
    x=>x.native_invoice_witness.billing_movement_id=id(50),x=>x.native_invoice_witness.upload_row_id=null,
    x=>x.native_invoice_witness.read_proof_hash='b'.repeat(64),x=>x.native_invoice_witness.owner='invented']) {
    const x=item();change(x);assert.throws(()=>projectChecklistItem(x,actor));
  }
});
test('invoice item cannot acquire a Timesheet, Queries or unproved Finalise action',()=>{
  for(const action of [{kind:'TIMESHEET',label:'Open Timesheet ›',native_allowed:true,target:{timesheet_id:id(8)}},
    {kind:'QUERIES',label:'View Queries ›',native_allowed:true,target:{...scope,row_key:'qg_'+H}},
    {kind:'FINALISE',label:'View Finalise ›',native_allowed:true,target:scope}]) {
    const x=item();x.action=action;assert.throws(()=>projectChecklistItem(x,actor));
  }
  const x=item();x.counted_timesheet_id=id(8);assert.throws(()=>projectChecklistItem(x,actor));
});
test('closed invoice item rejects role/route/subject relabel and foreign native witness',()=>{
  for(const change of [{route:'AGENCY_INVOICE'},{subject:'PAY',permitted_pay_subject:true},{reason_code:'REPORT_LINE_NET'},
    {native_source_witness:{}},{queries_parent_witness:{}},{submission_read:{}},{source_information_witness:{}}]) {
    assert.throws(()=>projectChecklistItem({...item(),...change},actor));
  }
  const x=item();x.item_kind='ACTUAL_TIMESHEET';x.counted_timesheet_id=id(8);assert.throws(()=>projectChecklistItem(x,actor));
});
test('finite invoice block authority cannot certify a whole main row or leak raw private frames',()=>{
  const cell={value:false,status:{text:'One item',tone:'muted',subtext:'',subtone:'muted'},item_count:1,link:null};
  assert.throws(()=>projectChecklistRow({row_id:'row',scope,client:'Client',group:'Group',route:'SELF_BILL',
    retention:{actual_timesheet_count:0,most_recent_ended_week:scope.week_ending,has_open_items:true,earlier_open_weeks:0},
    checked:cell,completed:cell,pay_completed:cell,hold:hold(),permitted_pay_item_count:0,admin_actions:[]},actor));
  const x=item();x.native_invoice_witness.native_facts={};assert.throws(()=>projectChecklistItem(x,actor));
});
test('proved invoice block cannot render a cleared/paid/decision badge from an inconsistent native status',()=>{
  for(const status of [{text:'Paid',tone:'positive'},{text:'Holds invoice',tone:'positive'},
    {text:'Decision needed',tone:'warning'},null])assert.throws(()=>projectChecklistItem({...item(),status},actor));
});
