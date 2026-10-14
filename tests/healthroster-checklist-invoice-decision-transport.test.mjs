import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {functionEvents,sqlTokens} from '../docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {decodeChecklistHold,projectChecklistItem,projectChecklistRow} from '../docs/healthroster-build/proposals/checklist-native-invoice-decision-transport-20261015/forward/healthroster-checklist-source.mjs';
import {decodeChecklistRead} from '../docs/healthroster-build/proposals/checklist-native-invoice-decision-transport-20261015/forward/checklist-read.mjs';
const base=new URL('../docs/healthroster-build/proposals/checklist-native-invoice-decision-transport-20261015/',import.meta.url),read=p=>fs.readFileSync(new URL(p,base),'utf8');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,H='a'.repeat(64),D='b'.repeat(64),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const scope={source_group_id:id(1),source_cycle_id:id(2),client_id:id(3),week_ending:'2026-10-11'},actor={actor_user_id:id(4),permission_view:'ADMIN',active:true};
function nativeItem(qualified=true){const item={item_id:'native-invoice:'+id(8),item_kind:'NATIVE_INVOICE',scope,route:'SELF_BILL',worker:'Actual Worker',
 work_date:'2026-10-07',sheet_scope:'WEEKLY',reason_code:'VAT_FIT',status:{text:'Holds invoice',tone:'danger',subtext:'Native invoice block',subtone:'muted'},
 hold:{holds_pay:null,holds_pay_scope:null,withheld_amount_pence:null,holds_invoice:true,pending_decision:qualified?true:null,
 authority:{owner:qualified?'SOURCE_INVOICE_DECISION':'SOURCE_INVOICE_BLOCK',hash:qualified?D:H,withholding_known:false}},
 subject:'INVOICE_ONLY',permitted_pay_subject:false,action:null,counted_timesheet_id:null,correction_binding:null,source_information_witness:null,submission_read:null,native_source_witness:null,
 native_invoice_witness:{owner:'private.weekly_source_invoice_unreleased_vat_fit_movements_v1',actor_user_id:actor.actor_user_id,scope,
 final_revision_id:id(6),client_manifest_id:id(7),billing_movement_id:id(8),upload_row_id:id(9),work_event_id:id(10),manifest_hash:H,movement_economic_hash:H,read_proof_hash:H}};
 if(qualified)item.native_invoice_decision_witness={owner:'private.weekly_source_invoice_hold_release_context_core_v1',actor_user_id:actor.actor_user_id,scope,
 billing_movement_id:id(8),client_manifest_id:id(7),upload_row_id:id(9),invoice_block_read_proof_hash:H,source_row_hash:H,authority_scope_version:7,
 hold_kind:'VAT_FIT',qualification_choice:'BILL_AT_NET_PLUS_20',read_proof_hash:D};return item;}
const request={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
function envelope(items,view='ADMIN'){return {ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command:request.command,actor_user_id:actor.actor_user_id,permission_view:view,
 binding:H,read_revision:D,rows:[],items,has_more:false,next_cursor:null,refresh_required:false,anchor:null};}
const call=item=>decodeChecklistRead(envelope([item]),request,{id:actor.actor_user_id,payView:false});
test('whole native transport preserves a qualified current invoice decision with independent unknown pay amount and no action',()=>{
 const x=call(nativeItem()).items[0];assert.equal(x.hold.pending_decision,true);assert.equal(x.hold.holds_invoice,true);assert.equal(x.hold.holds_pay,null);
 assert.equal(x.hold.withheld_amount_pence,null);assert.equal(x.action,null);assert.equal(x.counted_timesheet_id,null);assert.equal(x.native_invoice_witness.read_proof_hash,H);assert.equal(x.native_invoice_decision_witness.read_proof_hash,D);
});
test('unknown qualification preserves the complete base invoice item rather than suppressing the catalogue',()=>{
 const decoded=decodeChecklistRead(envelope([nativeItem(false),{...nativeItem(),item_id:'native-invoice:'+id(18),native_invoice_witness:{...nativeItem().native_invoice_witness,billing_movement_id:id(18)},native_invoice_decision_witness:{...nativeItem().native_invoice_decision_witness,billing_movement_id:id(18)}}]),request,{id:actor.actor_user_id,payView:false});
 assert.equal(decoded.items.length,2);assert.equal(decoded.items[0].hold.pending_decision,null);assert.equal(decoded.items[0].hold.holds_invoice,true);assert.equal(decoded.items[1].hold.pending_decision,true);
});
test('decision authority is finite positive invoice-only truth and cannot manufacture pay, known zero or cleared decision',()=>{
 const h=nativeItem().hold;for(const change of [{pending_decision:null},{pending_decision:false},{holds_invoice:false},{holds_invoice:null},
 {holds_pay:false,withheld_amount_pence:'0',authority:{...h.authority,withholding_known:true}},
 {holds_pay:true,holds_pay_scope:'TIMESHEET',withheld_amount_pence:'100',authority:{...h.authority,withholding_known:true}},
 {holds_pay_scope:'SHIFT'},{withheld_amount_pence:'0'},{authority:{...h.authority,withholding_known:true}}])assert.throws(()=>decodeChecklistHold({...h,...change}));
});
test('all new scalar witness hashes reject arrays, objects, null, number and boolean through the whole transport',()=>{
 for(const key of ['invoice_block_read_proof_hash','source_row_hash','read_proof_hash'])for(const value of [[H],[],{},null,17,true]){
  const x=nativeItem();x.native_invoice_decision_witness[key]=value;assert.throws(()=>call(x));}
 const x=nativeItem();x.hold.authority.hash=[D];assert.throws(()=>call(x));
});
test('actual decision actor scope upload movement and manifest must match the existing native block witness',()=>{
 for(const key of ['actor_user_id','billing_movement_id','client_manifest_id','upload_row_id']){const x=nativeItem();x.native_invoice_decision_witness[key]=id(99);assert.throws(()=>call(x));}
 for(const key of ['source_group_id','source_cycle_id','client_id','week_ending']){const x=nativeItem();x.native_invoice_decision_witness.scope={...scope,[key]:key==='week_ending'?'2026-10-18':id(99)};assert.throws(()=>call(x));}
});
test('native block proof and decision authority proof are separately bound and cannot be swapped',()=>{
 for(const change of [x=>x.native_invoice_decision_witness.invoice_block_read_proof_hash=D,x=>x.native_invoice_decision_witness.read_proof_hash=H,
 x=>x.native_invoice_decision_witness=null,x=>x.hold.authority.owner='SOURCE_INVOICE_BLOCK',x=>x.native_invoice_decision_witness.owner='private.invented']){
  const x=nativeItem();change(x);assert.throws(()=>call(x));}
});
test('only the exact native qualification purpose and safe actual authority version are admissible',()=>{
 for(const change of [{hold_kind:'DUPLICATE'},{qualification_choice:'BILL_AS_FILE'},{authority_scope_version:-1},{authority_scope_version:'7'},
 {authority_scope_version:1.2},{authority_scope_version:Number.MAX_SAFE_INTEGER+1},{native_facts:{}}]){
  const x=nativeItem();Object.assign(x.native_invoice_decision_witness,change);assert.throws(()=>call(x));}
});
test('decision cannot provide an unproved Finalise Timesheet or Queries action or row admission',()=>{
 for(const action of [{kind:'FINALISE',native_allowed:true,label:'View Finalise',target:scope},{kind:'TIMESHEET',native_allowed:true,label:'Timesheet',target:{timesheet_id:id(9)}},
 {kind:'QUERIES',native_allowed:true,label:'Queries',target:{...scope,row_key:'qg_'+H}}])assert.throws(()=>call({...nativeItem(),action}));
 assert.throws(()=>call({...nativeItem(),counted_timesheet_id:id(9)}));
});
test('noninvoice relabel, foreign subject route or raw frame leakage is refused',()=>{
 for(const change of [{item_kind:'ACTUAL_TIMESHEET',counted_timesheet_id:id(9)},{item_kind:'NATIVE_SOURCE'},{route:'AGENCY_INVOICE'},
 {subject:'PAY',permitted_pay_subject:true},{native_source_witness:{}},{queries_parent_witness:{}},{submission_read:{}},{source_information_witness:{}}])assert.throws(()=>call({...nativeItem(),...change}));
 const x=nativeItem();x.native_invoice_decision_witness.private_context={};assert.throws(()=>call(x));
});
test('reduced pay readers require the decision population filtered before paging',()=>{
 assert.equal(projectChecklistItem(nativeItem(),{...actor,permission_view:'PAY_READONLY'}),null);
 assert.throws(()=>decodeChecklistRead(envelope([nativeItem()],'PAY_READONLY'),request,{id:actor.actor_user_id,payView:true}),/NATIVE_FILTER_NOT_APPLIED/);
});
test('finite decision authority cannot certify a whole main row or a paid/completed status',()=>{
 const c={value:false,status:{text:'One item',tone:'muted'},item_count:1,link:null};assert.throws(()=>projectChecklistRow({row_id:'row',scope,client:'Client',group:'Group',route:'SELF_BILL',
 retention:{actual_timesheet_count:0,most_recent_ended_week:scope.week_ending,has_open_items:true,earlier_open_weeks:0},checked:c,completed:c,pay_completed:c,hold:nativeItem().hold,permitted_pay_item_count:0,admin_actions:[]},actor));
 for(const status of [{text:'Paid',tone:'positive'},{text:'Decision needed',tone:'warning'},null])assert.throws(()=>call({...nativeItem(),status}));
});
test('three complete transport producer owners reverse through nine literal hunks to exact frozen aea bytes',()=>{
 const p=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(p.proofs.length,3);assert.equal(p.proofs.reduce((n,x)=>n+x.hunks.length,0),9);
 for(const r of p.proofs){const b=fs.readFileSync(new URL(r.saved,base));assert.equal(sha(b),r.before_sha256);assert.ok(b.equals(execFileSync('git',['cat-file','blob',r.source_commit+':'+r.source_path],{maxBuffer:4e6})));
 let next=read(r.output);assert.equal(sha(next),r.after_sha256);for(const h of [...r.hunks].reverse()){assert.equal(next.split(h.after).length,2,h.label);next=next.replace(h.after,()=>h.before);}assert.equal(next,b.toString());}
});
test('scope-local reader delegates sole native owners in literal read mode, preserving role and unknown exception boundaries',()=>{
 const s=read('forward/wp_u3_source_invoice_decisions.sqlinc'),e=functionEvents(s).events.find(e=>e.definition),words=sqlTokens(e.body).filter(t=>t.kind==='word').map(t=>t.value);
 assert.deepEqual(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','round','sum'].includes(w)),[]);
 assert.equal((s.match(/weekly_source_invoice_hold_release_context_core_v1\(request,false\)/g)||[]).length,2);
 assert.equal((s.match(/weekly_source_invoice_hold_first_invoice_assert_v1\(row_id\)/g)||[]).length,2);
 assert.match(s,/exception when sqlstate '55000' or sqlstate '40001' or sqlstate '22023'/);assert.doesNotMatch(s,/when others|42501|request,true|hold_release_atomic_v1\(/);
 assert.match(s,/if sqlerrm='WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' then raise/);
 assert.match(s,/revoke all[\s\S]*public,anon,authenticated,service_role/);assert.equal(scanManagedSql(s).statements.length,3);
});
test('one census before all items and one after all items remove per-item whole-scope re-census',()=>{
 const s=read('forward/wp_u3_source_invoice_decisions.sqlinc'),loop=s.indexOf('for item in'),end=s.indexOf('  end loop;',loop),calls=[...s.matchAll(/weekly_source_checklist_source_invoice_block_items_v1\(p_scope,p_actor_user_id\)/g)].map(m=>m.index);
 assert.equal(calls.length,2);assert.ok(calls[0]<loop&&calls[1]>end);assert.match(s,/items:=items\|\|jsonb_build_array\(item\)/);
 assert.doesNotMatch(s,/\bcontinue\b|\blimit\s|pending_decision',false|withheld_amount_pence','0'/i);assert.match(s,/if blocks is distinct from again/);
});
test('whole producer consumes same decision frames before its actor-wide revision and page without changing pay process or admission',()=>{
 const s=read('forward/wp_u3_native_checklist.sqlinc'),begin=s.indexOf("if route='SELF_BILL' and view='ADMIN' then"),arm=s.slice(begin,s.indexOf("if route='AGENCY_INVOICE' then",begin));
 assert.match(arm,/weekly_source_checklist_source_invoice_decision_items_v1\(row_scope,actor\.id\)/);assert.match(arm,/financial_facts:=financial_facts\|\|jsonb_build_array\(invoice_blocks->'native_facts'\)/);
 assert.match(arm,/'action',null,'counted_timesheet_id',null/);assert.match(arm,/completed:=false/);assert.doesNotMatch(arm,/pay_complete:=|actual_count:=|pay_count:=/);
 assert.ok(begin<s.indexOf("'WEEKLY_SOURCE_CHECKLIST_READ_REVISION_V1'"));assert.ok(begin<s.indexOf('weekly_source_checklist_page_v1('));
});
test('complete successor source graph preserves every immutable native callee and exposes its conditional lock seam as unqualified',()=>{
 const graph=JSON.parse(read('NATIVE_READ_GRAPH.json'));assert.equal(graph.routines.length,49);assert.equal(graph.native,false);assert.equal(graph.SQL_executed,false);assert.equal(graph.public_root_mounted,false);
 for(const r of graph.routines){assert.equal(sha(read(r.saved_file)),r.definition_sha256);for(const id of r.callees)assert.ok(graph.routines.some(x=>x.id===id),id);}
 const flags=graph.routines.filter(r=>r.direct_flags.length||r.row_lock_sites);assert.equal(flags.length,1);assert.equal(flags[0].name,'private.weekly_source_invoice_hold_release_context_core_v1');
 assert.deepEqual(flags[0].direct_flags,['update']);assert.equal(flags[0].row_lock_sites,3);
 const purpose=graph.routines.find(r=>r.name==='private.weekly_source_checklist_source_invoice_decision_items_v1');assert.ok(purpose);assert.equal(purpose.callees.length,4);
});
test('native positive decision needs exact request hash version rules client group cycle and no release receipt; unavailable version preserves the base',()=>{
 const s=read('forward/wp_u3_source_invoice_decisions.sqlinc');
 for(const fragment of ["context->>'expected_source_row_hash' is distinct from request->>'expected_source_row_hash'",
 "context->>'expected_authority_scope_version' is distinct from request->>'expected_authority_scope_version'",
 "context->>'client_id' is distinct from p_scope->>'client_id'","context#>>'{cycle,id}' is distinct from p_scope->>'source_cycle_id'",
 "context#>>'{group,id}' is distinct from p_scope->>'source_group_id'","context#>>'{rules,source_amount_basis}' is distinct from 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'",
 "where r.upload_row_id=row_id and r.hold_kind='VAT_FIT'","cycle.version>9007199254740991"])assert.ok(s.includes(fragment),fragment);
 assert.match(s,/NATIVE_RELEASE_VERSION_TRANSPORT_UNAVAILABLE/);assert.match(s,/if known then[\s\S]*SOURCE_INVOICE_DECISION/);
});
