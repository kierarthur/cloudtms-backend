// Actual saved dispatcher/decoder with explicitly controlled native responses.
// No database, network, native scope, money, browser or qualification proof.
import test from 'node:test';import assert from 'node:assert/strict';
import {dispatchWeeklySourceRequest} from '../broker/src/weekly-source/routes.js';
import {checklistReadRequest,decodeChecklistRead} from '../broker/src/weekly-source/checklist-read.mjs';
import {unavailableChecklistHold} from '../broker/src/weekly-source/healthroster-checklist-source.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,H='a'.repeat(64),R='b'.repeat(64);
const scope={source_group_id:id(1),client_id:id(2),source_cycle_id:id(3),week_ending:'2026-10-11'};
const actor={id:id(4),role:'admin',is_active:true,payView:false};
const cell=value=>({value,status:{text:value?'Completed':'To do',tone:value?'positive':'danger',subtext:'',subtone:'muted'},item_count:0,link:null});
const hold=()=>({holds_pay:false,holds_pay_scope:null,withheld_amount_pence:'0',holds_invoice:false,pending_decision:false,authority:{owner:'SOURCE',hash:H,withholding_known:true}});
const row=()=>({row_id:`client-week:${id(1)}:${id(2)}:2026-10-11`,scope:{...scope},client:'Client',group:'Group',route:'SELF_BILL',retention:{actual_timesheet_count:0,most_recent_ended_week:'2026-10-11',has_open_items:false,earlier_open_weeks:0},checked:cell(true),completed:cell(true),pay_completed:cell(true),hold:hold(),permitted_pay_item_count:1,admin_actions:[]});
const item=()=>({item_id:`timesheet:${id(5)}`,item_kind:'ACTUAL_TIMESHEET',scope:{...scope},route:'SIGNED_TIMESHEETS',worker:'Worker',work_date:'2026-10-07',sheet_scope:'WEEKLY',reason_code:'SIGNED_NOT_AUTHORISED',status:{text:'Timesheet not authorised',tone:'danger',subtext:'',subtone:'muted'},hold:hold(),subject:'PAY_AND_INVOICE',permitted_pay_subject:true,action:null,counted_timesheet_id:id(5),correction_binding:null});
const frame=(command='CHECKLIST',view='ADMIN')=>({ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command,actor_user_id:actor.id,permission_view:view,binding:H,read_revision:R,rows:command==='CHECKLIST'?[row()]:[],items:command==='CHECKLIST_ISSUES'?[item()]:[],has_more:false,next_cursor:null,refresh_required:false,anchor:null});
const req=q=>checklistReadRequest(new URL('https://test.invalid/?'+q));
const issuesQuery=()=>new URLSearchParams({command:'CHECKLIST_ISSUES',...scope}).toString();
async function dispatch(query='',user=actor,payload=frame(),method='GET'){
 const calls=[];const response=await dispatchWeeklySourceRequest(new Request('https://test.invalid/api/weekly-source/v1/checklist?'+query,{method}),{}, {},{requireUser:async()=>user,rpc:async(...args)=>{calls.push(args);return payload;}});
 return {response,calls};
}
test('actual Checklist dispatcher invokes only one planned native reader with the fresh authenticated actor',async()=>{
 const {response,calls}=await dispatch();assert.equal(response.status,200);assert.equal(calls.length,1);assert.equal(calls[0][0],'weekly_source_checklist_v1');
 assert.deepEqual(calls[0][1],{p_request:{command:'CHECKLIST',sort:'week_ending',direction:'DESC',limit:50,actor_user_id:actor.id}});
 assert.equal(response.headers.get('cache-control'),'no-store');assert.equal((await response.json()).rows.length,1);
});
test('issue expansion uses the same reader with all four exact scope keys and bounded ascending worker traversal',async()=>{
 const {response,calls}=await dispatch(issuesQuery(),actor,frame('CHECKLIST_ISSUES'));assert.equal(response.status,200);assert.equal(calls[0][0],'weekly_source_checklist_v1');assert.equal(calls[0][1].p_request.command,'CHECKLIST_ISSUES');assert.equal(calls[0][1].p_request.direction,'ASC');assert.equal((await response.json()).items.length,1);
});
test('actor, view, financial/private facts and duplicate query fields are rejected before any native call',async()=>{
 for(const query of ['actor_user_id='+id(9),'permission_view=ADMIN','withheld_amount_pence=100','pending_decision=false','command=CHECKLIST&command=CHECKLIST_ISSUES','client_id='+id(2)+'&client_id='+id(3),'offset=50']){const {response,calls}=await dispatch(query);assert.equal(response.status,400,query);assert.equal(calls.length,0);}
});
test('invalid identities, civil dates, bounds and incomplete expansions refuse before RPC',async()=>{
 for(const query of ['client_id=bad','week_ending=2026-02-30','week_ending=1900-02-29','limit=0','limit=101','limit=1.5','limit=050','command=CHECKLIST_ISSUES','command=NO_ONE_WORKED','cursor='+encodeURIComponent('x'.repeat(4097)),'cursor=one&anchor=two']){const {response,calls}=await dispatch(query);assert.equal(response.status,400);assert.equal(calls.length,0);}
 assert.equal(req('week_ending=2024-02-29').week_ending,'2024-02-29');
});
test('all five main sorts and both directions pass unchanged; issue order has its own bounded native sort',()=>{
 for(const sort of ['week_ending','client','group','checked','completed'])for(const direction of ['ASC','DESC'])assert.equal(req(new URLSearchParams({sort,direction})).sort,sort);
 assert.throws(()=>req(issuesQuery()+'&sort=completed'));assert.throws(()=>req(issuesQuery()+'&direction=DESC'));assert.throws(()=>req('sort=worker'));
});
test('inactive, unapproved and absent current actors cannot call the native reader',async()=>{
 for(const user of [null,{...actor,is_active:false},{...actor,role:'staff',payment_authoriser:false,payment_golden_key:false}]){const {response,calls}=await dispatch('',user);assert.ok([401,403].includes(response.status));assert.equal(calls.length,0);}
});
test('approved non-admin payment actor receives only the exact native PAY_READONLY row view',async()=>{
 const pay={...actor,role:'staff',payment_authoriser:true,payView:true},payload=frame('CHECKLIST','PAY_READONLY');payload.rows[0].checked=cell(null);
 const {response,calls}=await dispatch('',pay,payload);assert.equal(response.status,200);assert.equal(calls[0][1].p_request.actor_user_id,actor.id);assert.ok(!Object.hasOwn(calls[0][1].p_request,'permission_view'));
 const result=await response.json();assert.equal(result.rows[0].checked.value,null);assert.deepEqual(result.rows[0].admin_actions,[]);assert.equal(result.rows[0].completed.link,null);
});
test('non-admin admin view, leaked row links and issue actions fail instead of postfiltering a native page',async()=>{
 const pay={...actor,role:'staff',payment_golden_key:true,payView:true};
 assert.equal((await dispatch('',pay,frame())).response.status,502);
 const linked=frame('CHECKLIST','PAY_READONLY');linked.rows[0].completed.link={kind:'FINALISE',label:'Finalise in Imports ›',target:{source_group_id:scope.source_group_id,client_id:scope.client_id,source_cycle_id:scope.source_cycle_id},native_allowed:true};
 const leaked=await dispatch('',pay,linked);assert.equal(leaked.response.status,502);assert.equal((await leaked.response.json()).error_code,'WEEKLY_SOURCE_CHECKLIST_PAY_ACTION_FORBIDDEN');
 const action=frame('CHECKLIST_ISSUES','PAY_READONLY');action.items[0].action={kind:'TIMESHEET',label:'Authorise in Timesheet ›',target:{timesheet_id:id(5)},native_allowed:true};assert.equal((await dispatch(issuesQuery(),pay,action)).response.status,502);
});
test('unknown withholding and pending decision remain NULL without invented zero or genuine decision',()=>{
 const payload=frame();payload.rows[0].hold=unavailableChecklistHold();const result=decodeChecklistRead(payload,req(''),actor);assert.equal(result.rows[0].hold.withheld_amount_pence,null);assert.equal(result.rows[0].hold.pending_decision,null);
 const falseDecision=frame();falseDecision.rows[0].hold={...unavailableChecklistHold(),pending_decision:true};assert.throws(()=>decodeChecklistRead(falseDecision,req(''),actor));
});
test('private raw DTOs, extra payload fields, foreign actor/scope and unknown cursor state are rejected',async()=>{
 for(const alter of [p=>p.private_rows=[],p=>p.rows[0].submission_witness={private:true},p=>p.actor_user_id=id(8),p=>p.has_more=true,p=>p.rows[0].scope.client_id=id(8),p=>p.binding=[H],p=>p.read_revision=[R]]){const payload=frame();alter(payload);assert.equal((await dispatch('client_id='+id(2),actor,payload)).response.status,502);}
});
test('changed revision explicitly requests anchored refresh rather than false end-of-list',async()=>{
 const payload=frame();payload.refresh_required=true;const {response}=await dispatch('',actor,payload);assert.equal(response.status,409);assert.equal((await response.json()).error_code,'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED');
});
test('duplicates, wrong main/expansion arm and nullable admin binary cells fail closed',async()=>{
 for(const alter of [p=>p.rows.push(row()),p=>p.items.push(item()),p=>p.rows[0].checked.value=null,p=>p.rows[0].completed.value=null]){const payload=frame();alter(payload);assert.equal((await dispatch('',actor,payload)).response.status,502);}
});
test('pre-keyset retention and pay subject omissions cannot be disguised by transport filtering',async()=>{
 const signed=frame();signed.rows[0].route='SIGNED_TIMESHEETS';assert.equal((await dispatch('',actor,signed)).response.status,502);
 const pay={...actor,role:'staff',payment_authoriser:true,payView:true},hidden=frame('CHECKLIST_ISSUES','PAY_READONLY');hidden.items[0].subject='INVOICE_ONLY';hidden.items[0].permitted_pay_subject=false;assert.equal((await dispatch(issuesQuery(),pay,hidden)).response.status,502);
});
test('a response-size failure is a contained read failure and never retries or calls a mutator',async()=>{
 const huge=frame();huge.rows=Array.from({length:100},(_,n)=>({...row(),row_id:'client-week:'+n,client:'x'.repeat(250),group:'y'.repeat(250),checked:{...cell(false),status:{text:'z'.repeat(2000),tone:'danger',subtext:'z'.repeat(2000),subtone:'muted'}}}));
 const {response,calls}=await dispatch('limit=100',actor,huge);assert.equal(response.status,502);assert.equal(calls.length,1);assert.equal(calls[0][0],'weekly_source_checklist_v1');
});
test('Checklist is GET-only; no No one worked or Undo is dispatched through the read route',async()=>{
 for(const method of ['POST','PUT','DELETE']){const {response,calls}=await dispatch('',actor,frame(),method);assert.equal(response.status,404);assert.equal(calls.length,0);}
});

test('native date and source-information fingerprint arrays cannot cross the closed read boundary',async()=>{
 const retained=frame();retained.rows[0].retention.most_recent_ended_week=['2026-10-11'];assert.equal((await dispatch('',actor,retained)).response.status,502);
 const dated=frame('CHECKLIST_ISSUES');dated.items[0].work_date=['2026-10-07'];assert.equal((await dispatch(issuesQuery(),actor,dated)).response.status,502);
 const information=frame('CHECKLIST_ISSUES');information.items[0]={...item(),item_id:'native-action:real-source-row',item_kind:'SOURCE_INFORMATION',counted_timesheet_id:null,subject:'INFORMATION',permitted_pay_subject:false,
   source_information_witness:{native_action_id:'real-source-row',evidence_fingerprint:H,import_id:id(8),hr_row_id:id(9),existing_row_id:'real-existing-row',counts_for_row_admission:false}};
 assert.equal((await dispatch(issuesQuery(),actor,information)).response.status,200);
 information.items[0].source_information_witness.evidence_fingerprint=[H];assert.equal((await dispatch(issuesQuery(),actor,information)).response.status,502);
});
