import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {functionEvents,sqlTokens} from '../docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {projectChecklistItem,projectChecklistRow} from '../docs/healthroster-build/proposals/checklist-invoice-decision-action-20261015/forward/healthroster-checklist-source.mjs';
import {decodeChecklistRead} from '../docs/healthroster-build/proposals/checklist-invoice-decision-action-20261015/forward/checklist-read.mjs';
import {dispatchWeeklySourceRequest} from '../broker/src/weekly-source/routes.js';
const base=new URL('../docs/healthroster-build/proposals/checklist-invoice-decision-action-20261015/',import.meta.url),read=p=>fs.readFileSync(new URL(p,base),'utf8');
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,H='a'.repeat(64),D='b'.repeat(64),sha=b=>crypto.createHash('sha256').update(b).digest('hex'),copy=v=>structuredClone(v);
const scope={source_group_id:id(1),source_cycle_id:id(2),client_id:id(3),week_ending:'2026-10-11'},actor={actor_user_id:id(4),permission_view:'ADMIN',active:true};
function descriptor(){return {contract:'WEEKLY_SOURCE_CHECKLIST_INVOICE_HOLD_DECISION_ACTION_V1',actor_user_id:actor.actor_user_id,permission_view:'ADMIN',scope:copy(scope),billing_movement_id:id(8),client_manifest_id:id(7),upload_row_id:id(9),expected_source_row_hash:H,expected_authority_scope_version:7,hold_kind:'VAT_FIT',decision_read_proof_hash:D,choices:['BILL_AT_NET_PLUS_20','BILL_AS_FILE']};}
const nativeAction=()=>({kind:'INVOICE_HOLD_DECISION',label:'Review invoice hold',native_allowed:true,target:descriptor()});
function item(){return {item_id:'native-invoice:'+id(8),item_kind:'NATIVE_INVOICE',scope:copy(scope),route:'SELF_BILL',worker:'Actual Worker',work_date:'2026-10-07',sheet_scope:'WEEKLY',reason_code:'VAT_FIT',
 status:{text:'Holds invoice',tone:'danger',subtext:'Native invoice block',subtone:'muted'},hold:{holds_pay:null,holds_pay_scope:null,withheld_amount_pence:null,holds_invoice:true,pending_decision:true,authority:{owner:'SOURCE_INVOICE_DECISION',hash:D,withholding_known:false}},
 subject:'INVOICE_ONLY',permitted_pay_subject:false,action:nativeAction(),counted_timesheet_id:null,correction_binding:null,source_information_witness:null,submission_read:null,native_source_witness:null,
 native_invoice_witness:{owner:'private.weekly_source_invoice_unreleased_vat_fit_movements_v1',actor_user_id:actor.actor_user_id,scope:copy(scope),final_revision_id:id(6),client_manifest_id:id(7),billing_movement_id:id(8),upload_row_id:id(9),work_event_id:id(10),manifest_hash:H,movement_economic_hash:H,read_proof_hash:H},
 native_invoice_decision_witness:{owner:'private.weekly_source_invoice_hold_release_context_core_v1',actor_user_id:actor.actor_user_id,scope:copy(scope),billing_movement_id:id(8),client_manifest_id:id(7),upload_row_id:id(9),invoice_block_read_proof_hash:H,source_row_hash:H,authority_scope_version:7,hold_kind:'VAT_FIT',qualification_choice:'BILL_AT_NET_PLUS_20',read_proof_hash:D}};}
const request={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
function decode(value){return decodeChecklistRead({ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command:request.command,actor_user_id:actor.actor_user_id,permission_view:'ADMIN',binding:H,read_revision:D,rows:[],items:[value],has_more:false,next_cursor:null,refresh_required:false,anchor:null},request,{id:actor.actor_user_id,payView:false});}
const payload=(choice='BILL_AT_NET_PLUS_20')=>({upload_row_id:id(9),source_cycle_id:id(2),report_scope_id:null,expected_source_row_hash:H,expected_authority_scope_version:7,hold_kind:'VAT_FIT',action:choice});
const receipt=(choice='BILL_AT_NET_PLUS_20')=>({ok:true,contract:'WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1',status:'RELEASED',idempotent:false,release_id:id(11),upload_row_id:id(9),hold_kind:'VAT_FIT',action:choice,net_pence:1000,vat_pence:200,gross_pence:1200});
test('whole strict transport admits only the actual native invoice decision descriptor and preserves independent unknown pay quantities',()=>{
 const result=decode(item()).items[0];assert.deepEqual(result.action,nativeAction());assert.equal(result.hold.holds_invoice,true);assert.equal(result.hold.pending_decision,true);assert.equal(result.hold.holds_pay,null);assert.equal(result.hold.withheld_amount_pence,null);assert.equal(result.counted_timesheet_id,null);
 const prior=item();prior.action=null;assert.equal(decode(prior).items[0].action,null);
});
test('new descriptor hashes and UUIDs reject scalar coercion arrays objects and all open target fields in complete transport',()=>{
 for(const key of ['expected_source_row_hash','decision_read_proof_hash','actor_user_id','billing_movement_id','client_manifest_id','upload_row_id'])for(const bad of [[descriptor()[key]],{},null,3,true]){const x=item();x.action.target[key]=bad;assert.throws(()=>decode(x));}
 for(const [key,bad] of [['expected_authority_scope_version','7'],['expected_authority_scope_version',-1],['expected_authority_scope_version',Number.MAX_SAFE_INTEGER+1],['choices','BILL_AS_FILE'],['choices',['BILL_AS_FILE','BILL_AT_NET_PLUS_20']],['choices',[['BILL_AT_NET_PLUS_20'],'BILL_AS_FILE']],['choices',['BILL_AT_NET_PLUS_20','BILL_AS_FILE','LEAVE_OFF']],['scope',[scope]],['hold_kind','DUPLICATE_REQUEST_ID'],['permission_view','PAY_READONLY'],['contract','FOREIGN'],['net_pence',1000]]){const x=item();x.action.target[key]=bad;assert.throws(()=>decode(x));}
});
test('foreign actor scope movement manifest row hash authority version and witness proof cannot be borrowed or swapped',()=>{
 for(const key of ['actor_user_id','billing_movement_id','client_manifest_id','upload_row_id']){const x=item();x.action.target[key]=id(99);assert.throws(()=>decode(x));}
 for(const key of Object.keys(scope)){const x=item();x.action.target.scope[key]=key==='week_ending'?'2026-10-18':id(99);assert.throws(()=>decode(x));}
 for(const change of [x=>x.action.target.expected_source_row_hash=D,x=>x.action.target.expected_authority_scope_version++,x=>x.action.target.decision_read_proof_hash=H,x=>x.native_invoice_decision_witness=null,x=>x.action.label='Finalise invoice',x=>x.action.native_allowed=false]){const x=item();change(x);assert.throws(()=>decode(x));}
});
test('finite invoice action cannot escape into another item, main row, readiness cell or reduced pay view',()=>{
 const x=item();assert.equal(projectChecklistItem(x,{...actor,permission_view:'PAY_READONLY'}),null);
 const cell={value:false,status:{text:'One item',tone:'muted'},item_count:1,link:nativeAction()};
 const row={row_id:'row',scope,client:'Client',group:'Group',route:'SELF_BILL',retention:{actual_timesheet_count:0,most_recent_ended_week:scope.week_ending,has_open_items:true,earlier_open_weeks:0},checked:cell,completed:cell,pay_completed:cell,hold:{holds_pay:null,holds_pay_scope:null,withheld_amount_pence:null,holds_invoice:null,pending_decision:null,authority:{owner:'UNAVAILABLE',hash:null,withholding_known:false}},permitted_pay_item_count:0,admin_actions:[]};
 assert.throws(()=>projectChecklistRow(row,actor));assert.throws(()=>projectChecklistRow({...row,checked:{...cell,link:null},completed:{...cell,link:null},pay_completed:{...cell,link:null},admin_actions:[nativeAction()]},actor));
 assert.throws(()=>decode({...x,item_kind:'ACTUAL_TIMESHEET',counted_timesheet_id:id(9)}));
});
test('new private action qualifiers reuse complete native read mode and first-invoice owners; unsupported choices keep the invoice catalogue',()=>{
 const s=read('forward/wp_u3_source_invoice_decisions.sqlinc'),words=sqlTokens(functionEvents(s).events.find(e=>e.definition).body).filter(t=>t.kind==='word').map(t=>t.value);
 assert.deepEqual(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','round','sum'].includes(w)),[]);
 assert.equal((s.match(/weekly_source_invoice_hold_release_context_core_v1\((?:request|file_request),false\)/g)||[]).length,4);assert.equal((s.match(/weekly_source_invoice_hold_first_invoice_assert_v1\(row_id\)/g)||[]).length,4);
 assert.match(s,/\(file_context-'choice'\) is distinct from \(context-'choice'\)/);assert.match(s,/native_release_choices_known/);assert.match(s,/'action',case when choices_known then/);
 assert.equal((s.match(/weekly_source_checklist_source_invoice_block_items_v1\(p_scope,p_actor_user_id\)/g)||[]).length,2);assert.doesNotMatch(s,/when others|42501|request,true|hold_release_atomic_v1\(|\bcontinue\b/i);assert.equal(scanManagedSql(s).statements.length,3);
});
test('whole native producer passes same owner action without invented Finalise target and binds all action facts before actor-wide revision/page',()=>{
 const s=read('forward/wp_u3_native_checklist.sqlinc'),a=s.indexOf("if route='SELF_BILL' and view='ADMIN' then"),b=s.indexOf("if route='AGENCY_INVOICE' then",a),arm=s.slice(a,b);
 assert.match(arm,/'action',raw->'action'/);assert.match(arm,/financial_facts:=financial_facts\|\|jsonb_build_array\(invoice_blocks->'native_facts'\)/);assert.doesNotMatch(arm,/'kind','FINALISE'|pay_complete:=|actual_count:=/);
 assert.ok(a<s.indexOf("'WEEKLY_SOURCE_CHECKLIST_READ_REVISION_V1'"));assert.ok(a<s.indexOf('weekly_source_checklist_page_v1('));
});
test('all six complete owners reverse to exact actual Git predecessors, retaining paid and R254 union bytes',()=>{
 const r=JSON.parse(read('COMPLETE_OWNER_REVERSES.json'));assert.equal(r.records.length,6);assert.equal(r.records.reduce((n,x)=>n+x.hunks.length,0),14);
 for(const p of r.records){const before=fs.readFileSync(new URL(p.saved,base));assert.equal(sha(before),p.before_sha256);assert.ok(before.equals(execFileSync('git',['cat-file','blob',p.commit+':'+p.file],{cwd:repo,maxBuffer:4e6})));let after=read(p.output);assert.equal(sha(after),p.after_sha256);
  for(const h of [...p.hunks].reverse()){assert.equal(after.split(h.after).length,2,h.label);after=after.replace(h.after,()=>h.before);}assert.equal(after,before.toString());}
 const graph=JSON.parse(read('NATIVE_READ_GRAPH.json'));assert.equal(graph.routines.length,49);for(const r of graph.routines)assert.equal(sha(read(r.saved_file)),r.definition_sha256);
 for(const f of ['forward/workspace-actions.jsinc','forward/import-workspace.jsinc'])new vm.Script(read(f));
});
test('existing whole Worker command route sends only native CAS request with authenticated actor and rejects Pay or inactive actors',async()=>{
 const calls=[],user={id:actor.actor_user_id,role:'admin',is_active:true,payView:false};
 const invoke=async(acting,choice='BILL_AT_NET_PLUS_20',extra={})=>{const response=await dispatchWeeklySourceRequest(new Request('https://test.invalid/api/weekly-source/v1/commands',{method:'POST',body:JSON.stringify({action:'RELEASE_INVOICE_HOLD',payload:{...payload(choice),...extra}})}),{}, {},{requireUser:async()=>acting,rpc:async(name,args)=>{calls.push({name,args});return receipt(choice);}});return{status:response.status,value:await response.json()};};
 for(const choice of descriptor().choices){assert.equal((await invoke(user,choice)).status,200);assert.deepEqual(calls.at(-1),{name:'weekly_source_invoice_hold_release_atomic_v1',args:{p_request:{...payload(choice),actor_user_id:user.id}}});}
 assert.equal((await invoke(user,'BILL_AS_FILE',{actor_user_id:id(99)})).status,200);assert.equal(calls.at(-1).args.p_request.actor_user_id,user.id);
 const n=calls.length;for(const acting of [{...user,role:'staff',payment_authoriser:true},{...user,role:'staff'},{...user,is_active:false}])assert.ok((await invoke(acting)).status>=400);
 assert.equal((await invoke(user,'BILL_AS_FILE',{pay_amount:10})).status,400);assert.equal(calls.length,n);
});
const officeRepo=path.resolve(repo,'../healthroster-v2-office-13102026');
const main=execFileSync('git',['cat-file','blob','2d15d121ac888a2e2a076dc7a2c4be85f9958556:js/main.js'],{cwd:officeRepo,maxBuffer:24000000});assert.equal(sha(main),'d86f2ef841f53243997c7f5bb73b913a4a04a2c563378ab47e58d1a5ee82c847');
const raw=main.toString(),start=raw.indexOf('function showModal(title, tabs, renderTab, onSave, hasId, onReturn, options) {'),hostCode=raw.slice(start,raw.indexOf('function applyTimesheetLifecyclePatchToModal(',start));assert.ok(hostCode.includes('stack().push(frame);'));
function fixture(reply=async()=>receipt()){
 const calls={requests:[],refresh:0,capture:0,toast:[],timers:[],tabs:[]},nodes=new Map(),parent={_token:'parent',kind:'weekly-source-imports-v1',setTab:async tab=>{calls.tabs.push(tab);}};
 class Element{constructor(){this.style={};this.dataset={};this.classList={toggle(){},add(){},remove(){},contains:()=>false};this.innerHTML='';this.listeners={};this.disabled=false;}
  setAttribute(){}removeAttribute(){}getAttribute(){return null;}querySelector(sel){if(!nodes.has(sel))nodes.set(sel,new Element());return nodes.get(sel);}querySelectorAll(sel){return sel==='[data-wsa-invoice-choice]'?choices:[];}addEventListener(name,fn){this.listeners[name]=fn;}removeEventListener(){}appendChild(){}
  click(){if(this===nodes.get('btnCloseModal'))root.closeModal();else return this.listeners.click?.();}}
 const choices=['BILL_AT_NET_PLUS_20','BILL_AS_FILE'].map(value=>Object.assign(new Element(),{value})),host=new Element();
 const document={getElementById:id=>{if(id==='irv1Review')return null;if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);},createElement:()=>new Element(),querySelector:sel=>sel==='[data-wsa-screen="invoice-decision"]'?host:null,querySelectorAll:()=>[],addEventListener(){},removeEventListener(){},body:{children:[]},activeElement:null};
 const root={__modalStack:[parent],document,addEventListener(){},removeEventListener(){},__LOG_MODAL:false,__toast:value=>calls.toast.push(value),html:v=>v,closeModal:()=>root.__modalStack.pop(),
  CloudTMSWeeklySourceImportWorkspaceV1:{},CloudTMSWeeklySourceWorkspaceActionsV1:{},authFetch:async(url,options)=>{const request=JSON.parse(options.body);calls.requests.push(copy(request));return{ok:true,json:()=>reply(request)};}};
 const ctx=vm.createContext({globalThis:root,window:root,document,HTMLElement:Element,byId:id=>document.getElementById(id),qs:()=>null,qsa:()=>[],module:{exports:{}},URL,URLSearchParams,TextEncoder,TextDecoder,
  SESSION:{user:{id:actor.actor_user_id,role:'admin',is_active:true,roles:['admin']}},setTimeout:fn=>{calls.timers.push(fn);return calls.timers.length;},clearTimeout(){},console:{log(){},warn(){},error(){},groupCollapsed(){},groupEnd(){}}});
 const source=read('forward/import-workspace.jsinc'),needle='_test: Object.freeze({ exactAcceptSystemHoursPayload,';assert.equal(source.split(needle).length,2);
 vm.runInContext(source.replace(needle,'_test: Object.freeze({ openChecklistAction, exactAcceptSystemHoursPayload,'),ctx);const workspace=ctx.module.exports;root.CloudTMSWeeklySourceImportWorkspaceV1=workspace;
 workspace._session.workspace=workspace.normaliseWorkspace({profile:{},context:{},imports:{},queries:{},finalise:{ready:{},blocked:{}},history:{}});workspace._session.activeTab='checklist';workspace._session.openCurrent=()=>true;
 workspace._session.checklist={capture:()=>calls.capture++,refresh:async()=>{calls.refresh++;}};
 vm.runInContext(read('forward/workspace-actions.jsinc'),ctx);const actions=ctx.module.exports;root.CloudTMSWeeklySourceWorkspaceActionsV1=actions;vm.runInContext(hostCode,ctx);root.showModal=ctx.showModal;
 return{ctx,root,calls,host,nodes,parent,workspace,actions,choices,open:()=>workspace._test.openChecklistAction(nativeAction(),{client:'Real Client',scope}),
  confirm:()=>host.querySelector('[data-wsa-invoice-confirm]').listeners.change({target:{checked:true}}),
  choose:value=>{const input=choices.find(x=>x.value===value);input.listeners.change();},
  submit:()=>host.querySelector('[data-wsa-invoice-submit]').listeners.click(),
  cancel:()=>host.querySelector('[data-wsa-invoice-close]').listeners.click()};
}
const mutations=[f=>vm.runInContext('SESSION={user:SESSION.user};',f.ctx),f=>vm.runInContext('SESSION.user={...SESSION.user};',f.ctx),f=>vm.runInContext("SESSION.user.id='00000000-0000-4000-8000-000000000099';",f.ctx),f=>vm.runInContext("SESSION.user.roles.push('withdrawn');",f.ctx),f=>vm.runInContext("SESSION.user.role='pay_readonly';",f.ctx),f=>vm.runInContext('SESSION.user.is_active=false;',f.ctx),f=>{f.parent._token='changed';},f=>{f.root.__modalStack[0]={...f.parent};},f=>{f.root.__modalStack.at(-1)._token='changed-child';},f=>{f.root.__modalStack.push({_token:'foreign',kind:'weekly-source-checklist-invoice-decision-v1'});},f=>{f.root.CloudTMSWeeklySourceImportWorkspaceV1={...f.workspace};},f=>{f.workspace._session.openGeneration++;},f=>{f.workspace._session.openCurrent=()=>false;},f=>{f.workspace._session.workspace={...f.workspace._session.workspace};}];
test('whole actual Checklist→Office→requestJson caller uses real push-before-render host and native default choice without browser money',async()=>{
 const f=fixture();await f.open();assert.match(f.nodes.get('modalBody').innerHTML,/Bill at net \+ 20%/);assert.match(f.nodes.get('modalBody').innerHTML,/Bill as in the file/);assert.equal(f.calls.capture,1);assert.equal(f.calls.requests.length,0);
 f.confirm();await f.submit();assert.deepEqual(f.calls.requests,[{action:'RELEASE_INVOICE_HOLD',payload:payload()}]);assert.equal(f.calls.refresh,1);assert.deepEqual(f.calls.toast,['Invoice hold released.']);assert.equal(f.root.__modalStack.length,1);
});
test('actual whole caller supports separately native-qualified file choice and cancellation sends no command',async()=>{
 const f=fixture(async request=>receipt(request.payload.action));await f.open();f.choose('BILL_AS_FILE');f.confirm();await f.submit();assert.deepEqual(f.calls.requests,[{action:'RELEASE_INVOICE_HOLD',payload:payload('BILL_AS_FILE')}]);assert.equal(f.calls.refresh,1);
 const c=fixture();await c.open();c.cancel();assert.equal(c.calls.requests.length,0);assert.equal(c.root.__modalStack.length,1);
});
test('whole caller freezes an unconfirmed choice for exact retry and accepts native idempotent replay without repeating another business save',async()=>{
 let attempt=0;const f=fixture(async request=>{if(!attempt++)throw Error('connection lost');return {...receipt(request.payload.action),idempotent:true};});await f.open();f.choose('BILL_AS_FILE');f.confirm();await f.submit();assert.equal(f.calls.refresh,0);assert.equal(f.root.__modalStack.length,2);
 f.choose('BILL_AT_NET_PLUS_20');f.confirm();await f.submit();assert.equal(f.calls.requests.length,2);assert.deepEqual(f.calls.requests[0],f.calls.requests[1]);assert.equal(f.calls.requests[1].payload.action,'BILL_AS_FILE');assert.equal(f.calls.refresh,1);
});
test('all actual actor session permission workspace and exact parent/child replacements refuse before dispatch',async()=>{
 for(const mutate of mutations){const f=fixture();await f.open();f.confirm();mutate(f);await f.submit();f.cancel();assert.equal(f.calls.requests.length,0);assert.equal(f.calls.refresh,0);assert.deepEqual(f.calls.toast,[]);}
});
test('all replacements while the actual command is pending discard old completion, close, toast and return refresh',async()=>{
 for(const mutate of mutations){let resolve;const pending=new Promise(done=>{resolve=done;}),f=fixture(()=>pending);await f.open();f.confirm();const run=f.submit();assert.equal(f.calls.requests.length,1);mutate(f);const length=f.root.__modalStack.length;resolve(receipt());await run;
  assert.equal(f.calls.refresh,0);assert.deepEqual(f.calls.toast,[]);assert.equal(f.root.__modalStack.length,length);}
});
test('wrong or unsafe native receipts and PGRST202/stale refusals remain unconfirmed with no automatic second command',async()=>{
 const invalid=[()=>null,()=>({...receipt(),contract:'OTHER'}),()=>({...receipt(),upload_row_id:id(99)}),()=>({...receipt(),action:'BILL_AS_FILE'}),()=>({...receipt(),gross_pence:Number.MAX_SAFE_INTEGER+1}),()=>({...receipt(),net_pence:'1000'}),()=>({...receipt(),release_id:[id(11)]}),()=>({...receipt(),private_frame:{}}),()=>{throw Object.assign(Error('missing'),{code:'PGRST202'});},()=>{throw Object.assign(Error('stale'),{code:'40001'});}];
 for(const response of invalid){const f=fixture(async()=>response());await f.open();f.confirm();await f.submit();assert.equal(f.calls.requests.length,1);assert.equal(f.calls.refresh,0);assert.equal(f.root.__modalStack.length,2);assert.deepEqual(f.calls.toast,[]);assert.match(f.nodes.get('modalBody').innerHTML,/Retry exact request/);}
});
test('Office descriptor validator agrees on scalar/closed ABI and rejects initial inactive/nonadmin/foreign proof before opening its decision child',async()=>{
 const ctx=vm.createContext({});vm.runInContext(read('office-invoice-decision.jsinc'),ctx);assert.ok(ctx.normaliseChecklistInvoiceDecision(descriptor(),actor.actor_user_id));
 for(const key of ['expected_source_row_hash','decision_read_proof_hash','actor_user_id','scope','choices']){const d=descriptor();d[key]=[d[key]];assert.equal(ctx.normaliseChecklistInvoiceDecision(d,actor.actor_user_id),null);}
 for(const code of ["SESSION.user.is_active=false;","SESSION.user.role='pay_readonly';"]){const f=fixture();vm.runInContext(code,f.ctx);await f.open();assert.equal(f.calls.requests.length,0);assert.doesNotMatch(f.nodes.get('modalBody')?.innerHTML||'',/data-wsa-invoice-submit/);}
});
