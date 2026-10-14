import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import assert from 'node:assert/strict';
import { composeOwnerSource, sourceSha256 } from '../scripts/healthroster-owner-source-compose.mjs';
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const folder=path.join(repo,'docs/healthroster-build/proposals/office-paid-native-caller-union-20261014');
const read=file=>fs.readFileSync(path.join(folder,file),'utf8');
const uid=n=>'10000000-0000-4000-8000-'+String(n).padStart(12,'0'),hash='a'.repeat(64);
function payload(){const question={schema_version:'WEEKLY_SOURCE_PAID_MATCH_QUESTION_V1',state:'READY',reason_code:null,
 upload_row_id:uid(1),upload_id:uid(2),projection_publication_id:uid(3),candidate_id:uid(4),client_id:uid(5),contract_id:uid(6),
 expected_authority_scope_version:2,expected_row_manifest_hash:hash,expected_paid_overlap_inputs_sha256:'b'.repeat(64),
 choices:[{work_event_id:uid(7),work_date:'2026-10-05',start:'08:00',end:'16:00',break_minutes:30,contract_id:uid(6),same_allowed:true}]};
 return{paid_match_question:question,upload_row_id:question.upload_row_id,contract_id:question.contract_id,
 candidate:'Candidate test',client:'Client test',shift:'5 Oct 2026',system_hours:'08:00–17:00 · 30 min break',
 recheck_payload:{request_id:uid(10),upload_id:uid(2),upload_row_id:uid(1),projection_publication_id:uid(3),
 expected_authority_scope_version:2,expected_row_manifest_hash:hash}};}
const officeRepo=path.join(path.dirname(repo),'healthroster-v2-office-13102026');
const main=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',
 '2d15d121ac888a2e2a076dc7a2c4be85f9958556:js/main.js'],{cwd:officeRepo,windowsHide:true,maxBuffer:24000000}).toString();
const hostStart=main.indexOf('function showModal(title, tabs, renderTab, onSave, hasId, onReturn, options) {');
const modalHost=main.slice(hostStart,main.indexOf('function applyTimesheetLifecyclePatchToModal(',hostStart));
assert.ok(hostStart>=0&&modalHost.includes('stack().push(frame);'));
function fixture({original=false,reply}={}){
 const calls=[],timers=[],elements=new Map(),nodes=new Map(),parent={_token:'real-parent'},stack=[parent];
 class Element{constructor(){this.style={};this.dataset={};this.classList={toggle(){},add(){},remove(){},contains:()=>false};this.innerHTML='';}
  setAttribute(){}removeAttribute(){}getAttribute(){return null;}querySelector(){return null;}querySelectorAll(){return [];}addEventListener(){}removeEventListener(){}appendChild(){}}
 const host={dataset:{},querySelector:key=>elements.get(key)};
 for(const key of ['target','choice','reason','save','cancel'])elements.set('[data-paid-match-'+key+']',
  {listeners:{},addEventListener(event,callback){this.listeners[event]=callback;}});
 const document={getElementById:id=>{if(!nodes.has(id))nodes.set(id,new Element());
   if(id==='btnCloseModal')nodes.get(id).click=()=>{stack.pop();closed++;};return nodes.get(id);},
  querySelector:selector=>selector==='[data-paid-match]'?host:null,querySelectorAll:()=>[],createElement:()=>new Element(),
  addEventListener(){},removeEventListener(){},body:{children:[]},activeElement:null};
 const authority={issueCommand:async(action,request)=>{calls.push({action,request});return reply?reply():
  {ok:true,status:'CURRENT',publication_id:uid(11),upload_id:uid(2)};},refresh:async()=>calls.push({refresh:true}),isSourceAdmissionBusy:()=>false};
 let closed=0;
 const ctx=vm.createContext({module:{exports:{}},SESSION:{user:{id:uid(12),role:'admin',roles:['admin'],is_active:true}},
  console:{log(){},warn(){},error(){},groupCollapsed(){},groupEnd(){}},URL,URLSearchParams,TextEncoder,TextDecoder,structuredClone,
  crypto:{randomUUID:crypto.randomUUID},setTimeout:callback=>{timers.push(callback);return 1;},clearTimeout(){},
  CloudTMSWeeklySourceImportWorkspaceV1:authority,__modalStack:stack,__LOG_MODAL:false,
  document,HTMLElement:Element,byId:id=>document.getElementById(id),qs:()=>null,qsa:()=>[],
  addEventListener(){},removeEventListener(){},closeModal:()=>{stack.pop();closed++;}});
 vm.runInContext('window=globalThis;',ctx);
 vm.runInContext(modalHost,ctx);
 vm.runInContext(read(original?'reference/workspace-actions.paid-native-before-guard.jsinc':'forward/workspace-actions.jsinc'),ctx,{timeout:3000});
 const api=ctx.module.exports,flush=()=>{let n=0;while(timers.length){if(++n>100)throw Error('UNBOUNDED_FIXTURE_TIMER');timers.shift()();}};
 const change=(key,value)=>elements.get('[data-paid-match-'+key+']').listeners[key==='reason'?'input':'change']?.({target:{value}});
 const open=()=>{api.handleAction({label:'Confirm paid shift match',payload:payload()});flush();};
 const prepare=()=>{open();change('target',uid(7));flush();change('choice','SAME');change('reason','Same clinical work');};
 return{ctx,api,calls,stack,parent,authority,flush,change,open,prepare,closed:()=>closed,rendered:()=>nodes.get('modalBody').innerHTML,
  click:async()=>{await elements.get('[data-paid-match-save]').listeners.click?.();flush();},
  cancel:()=>elements.get('[data-paid-match-cancel]').listeners.click?.()};
}
const mutations=[
 o=>vm.runInContext('SESSION={user:SESSION.user};',o.ctx),
 o=>vm.runInContext('SESSION.user={...SESSION.user};',o.ctx),
 o=>vm.runInContext("SESSION.user.roles.push('withdrawn');",o.ctx),
 o=>vm.runInContext("SESSION.user.role='pay_readonly';",o.ctx),
 o=>vm.runInContext('SESSION.user.is_active=false;',o.ctx),
 o=>{o.parent._token='replaced';},
 o=>{o.stack[0]={...o.parent};},
 o=>{o.stack.at(-1)._token='replaced-child';},
 o=>{o.stack.push({_token:'foreign',kind:'weekly-source-paid-match-v1'});},
 o=>{o.ctx.CloudTMSWeeklySourceImportWorkspaceV1={...o.authority};}
];

test('both complete Office composition layers reverse exactly and preserve the generic protected caller',()=>{
 const native=read('reference/workspace-actions.native-r254-before.jsinc');
 const combined=read('reference/workspace-actions.paid-native-before-guard.jsinc'),final=read('forward/workspace-actions.jsinc');
 for(const [file,before,after]of[['PAID_NATIVE_WHOLE_REVERSE.json',native,combined],['PAID_CALLER_WHOLE_REVERSE.json',combined,final]]){
  const record=JSON.parse(read(file));assert.equal(composeOwnerSource({source:before,expectedSha256:record.before_sha256,hunks:record.hunks}).output,after);}
 const caller=source=>source.slice(source.indexOf('  function protectedCaller('),source.indexOf('  async function finishAction('));
 assert.equal(caller(final),caller(native));new vm.Script(final);new vm.Script(read('forward/import-workspace.jsinc'));
});

test('complete actual pinned showModal pushes and renders the valid paid child, then submits the unchanged exact native request',async()=>{
 const o=fixture();o.prepare();assert.match(o.rendered(),/Confirm and recheck/);assert.equal(o.calls.length,0);
 await o.click();assert.equal(o.closed(),1);assert.equal(o.stack.length,1);
 assert.equal(o.calls[0].action,'RECHECK_SOURCE');assert.equal(o.calls[0].request.pay_effect,'NONE');
 assert.equal(o.calls.filter(row=>row.refresh).length,1);assert.equal(o.calls[0].request.work_event_id,uid(7));
 assert.ok(!('actor_user_id'in o.calls[0].request));
});

test('same-ID session replacement is reproduced in the preserved predecessor and refused in the complete successor',async()=>{
 for(const original of [true,false]){const o=fixture({original});o.prepare();mutations[0](o);await o.click();
  assert.equal(o.calls.filter(row=>row.action).length,original?1:0);assert.equal(o.closed(),original?1:0);}
});

test('all session user permission workspace and exact parent/child replacements refuse before request',async()=>{
 for(const mutate of mutations){const o=fixture();o.prepare();mutate(o);await o.click();
  assert.equal(o.calls.length,0);assert.equal(o.closed(),0);o.cancel();assert.equal(o.closed(),0);}
});

test('all replacements while the native request is pending refuse old receipt completion or refresh',async()=>{
 for(const mutate of mutations){let resolve;const pending=new Promise(done=>{resolve=done;});const o=fixture({reply:()=>pending});
  o.prepare();const running=o.click();assert.equal(o.calls.length,1);mutate(o);
  resolve({ok:true,status:'CURRENT',publication_id:uid(11),upload_id:uid(2)});await running;
  assert.equal(o.closed(),0);assert.equal(o.calls.filter(row=>row.refresh).length,0);}
});

test('unconfirmed save retries the same immutable native request after a valid unchanged continuation',async()=>{
 const o=fixture({reply:()=>{const e=new Error('incomplete');e.code='WEEKLY_SOURCE_RECHECK_INCOMPLETE';throw e;}});
 o.prepare();await o.click();o.change('reason','Different');await o.click();
 assert.equal(o.calls.length,2);assert.strictEqual(o.calls[0].request,o.calls[1].request);
 assert.equal(o.calls[1].request.match_reason,'Same clinical work');assert.equal(o.closed(),0);
});

test('source manifest validates all exact whole owner and reverse artifacts',()=>{
 for(const row of JSON.parse(read('SOURCE_MANIFEST.json')).entries){const b=fs.readFileSync(path.join(folder,row.path));
  assert.equal(b.length,row.bytes);assert.equal(sourceSha256(b),row.sha256);}
});
