import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {readTimesheetCorrectionOfficeContext} from './reference/backend/broker/src/timesheet-correction-office-context.mjs';
import {weeklySourceAuthoriseRouting} from '../install2-a0-scoped-pay-permission-successor-20261014/reference/broker/src/weekly-source/authorise-routing.mjs';
import {id,I,H} from '../install2-a0-runtime-route-wiring-20261014/unit-fixtures.mjs';
const env=JSON.parse(fs.readFileSync(new URL('../install2-a0-auth-tenant-successor-20261014/CURRENT_AUTH_OWNER_SOURCE.json',import.meta.url))).config.nonsecret_test_vars;
const admin={id:I.actor,is_active:true,role:'admin',tenant_id:env.BANKING_PAY_CORRECTION_TENANT_ID_V1};
const pay={...admin,role:'operator',payment_authoriser:true};
const request=()=>new Request(`https://unit.invalid/api/timesheets/${I.p0}/corrections/context`);
const descriptor={schema_version:1,candidate_id:I.candidate,request_id:I.request,root_timesheet_id:I.root,
 reversed_timesheet_id:I.original,replacement_timesheet_id:I.p0,correction_id:I.correction,origin:'SIGNED_TIMESHEET_CORRECTION',state:'WAITING',state_revision:1,
 reason:'UNIT_ONLY private reason',frozen_daily_receipt:{private:'UNIT_ONLY'},original_schedule:[{private:'UNIT_ONLY'}],proposed_schedule:[{}],target_revision:H};
const authority={contract_version:'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1',request_id:I.request,state:'WAITING',state_revision:'1',replacement_timesheet_id:I.p0,return_invoice_id:I.invoice};
function harness(options={}){
 const log=[],users=options.users||[admin];let n=0;
 const owners={requireUser:async(_,__,roles)=>{log.push(['actor',roles]);return users[Math.min(n++,users.length-1)];},
  assertScope:async({timesheetId,user})=>{log.push(['scope',timesheetId,user.id]);const scope=await weeklySourceAuthoriseRouting(async()=>{
   if(options.scopeError)throw options.scopeError;return options.scope??{contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:true};},timesheetId,user.id);
   if(scope.refusal){const e=new Error(scope.refusal.error_code);e.code=e.message;e.status=scope.refusal.status;throw e;}
   if(!scope.bound){const e=new Error('CORRECTION_SOURCE_SCOPE_NOT_ALLOWED');e.code=e.message;e.status=403;throw e;}},
  readContexts:async ids=>{log.push(['contexts',ids]);return options.contexts??[{effective_timesheet_id:I.p0,out_candidate:{id:I.candidate},
   out_timesheet:{timesheet_id:I.p0,is_current:true,archived_at_utc:null,version:3,correction_kind:'WHOLE_TIMESHEET_REPLACEMENT',correction_id:I.correction}}];},
  readCreateProjection:async args=>{log.push(['create',args]);return Object.hasOwn(options,'create')?options.create:descriptor;},
  readRequestProjection:async args=>{log.push(['projection',args]);return Object.hasOwn(options,'projection')?options.projection:descriptor;},
  readRequestAuthority:async args=>{log.push(['authority',args]);return options.authority??authority;}};
 return {log,owners,run:(e=env,r=request(),target=I.p0)=>readTimesheetCorrectionOfficeContext(e,r,target,owners)};
}
test('ADMIN read exposes only real existing native identity/state/CAS, current target version and narrow invoice ID',async()=>{
 const h=harness(),r=await h.run();assert.equal(r.view,'ADMIN');assert.equal(r.target.timesheet_version,3);
 assert.deepEqual(Object.keys(r.request).sort(),['request_id','correction_id','origin','state','state_revision','root_timesheet_id','reversed_timesheet_id','replacement_timesheet_id','return_invoice_id'].sort());
 assert.equal(r.request.return_invoice_id,I.invoice);assert.equal(r.request.state_revision,1);
 for(const forbidden of ['private reason','frozen_daily_receipt','schedule','target_revision','finance','can_send','can_authorise','qualification'])assert.ok(!JSON.stringify(r).includes(forbidden));
 assert.deepEqual(h.log.map(c=>c[0]),['actor','contexts','create','projection','authority','actor']);
});
test('PAY read probes real target before context/readers and on fresh recheck, omitting invoice and management descriptors',async()=>{
 const h=harness({users:[pay]}),r=await h.run();assert.equal(r.view,'PAY');assert.equal(r.create_descriptor,null);assert.ok(!Object.hasOwn(r.request,'return_invoice_id'));
 assert.deepEqual(h.log.map(c=>c[0]),['actor','scope','contexts','projection','authority','actor','scope']);
 assert.ok(!h.log.some(c=>c[0]==='create'));assert.ok(!JSON.stringify(r).includes(I.invoice));
 for(const flag of ['payment_authoriser','payment_golden_key']){const actor={...pay,payment_authoriser:false,payment_golden_key:false,[flag]:true};assert.equal((await harness({users:[actor]}).run()).view,'PAY');}
});
test('no current request/create authority is a genuine null and cannot mint fresh SEND eligibility',async()=>{
 const h=harness({create:null,projection:null}),r=await h.run();assert.equal(r.create_descriptor,null);assert.equal(r.request,null);assert.ok(!h.log.some(c=>c[0]==='authority'));
 assert.ok(!Object.hasOwn(r,'can_send'));assert.ok(!Object.hasOwn(r,'actions'));
});
test('inactive/unflagged/string-flags/foreign tenant and nonTEST refuse before readers',async()=>{
 for(const user of [null,{...pay,is_active:false},{...pay,payment_authoriser:false},{...pay,payment_authoriser:'true'},{...pay,tenant_id:env.MYTMS_OFFICE_AGENCY_ID}]){
  const h=harness({users:[user]});await assert.rejects(h.run(),e=>e.status===403);assert.equal(h.log.length,1);
 }
 const h=harness();await assert.rejects(h.run({...env,CANDIDATE_APP_ENVIRONMENT:'LIVE'}),/CORRECTION_ENVIRONMENT_NOT_ALLOWED/);assert.equal(h.log.length,1);
});
test('ordinary/Daily nonapplicable and unknown/PGRST202 scopes cannot expose private request context',async()=>{
 for(const opts of [{scope:{contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:false}},{scope:{}},{scopeError:{code:'PGRST202'}}]){
  const h=harness({users:[pay],...opts});await assert.rejects(h.run(),e=>[403,503].includes(e.status));assert.deepEqual(h.log.map(c=>c[0]),['actor','scope']);
 }
});
test('native physical target must resolve exactly current active identity/candidate/version',async()=>{
 for(const contexts of [[],[{effective_timesheet_id:I.original,out_timesheet:{},out_candidate:{id:I.candidate}}],
  [{effective_timesheet_id:I.p0,out_timesheet:{timesheet_id:I.p0,is_current:false,version:3},out_candidate:{id:I.candidate}}],
  [{effective_timesheet_id:I.p0,out_timesheet:{timesheet_id:I.p0,is_current:true,version:9007199254740992},out_candidate:{id:I.candidate}}]])
  await assert.rejects(harness({contexts}).run(),/CORRECTION_CONTEXT_TARGET_UNPROVED/);
});
test('foreign candidate/request membership or malformed projection refuses before native authority',async()=>{
 for(const projection of [{...descriptor,candidate_id:id(90)},{...descriptor,request_id:id(91)},{...descriptor,state_revision:9007199254740992},
  {...descriptor,replacement_timesheet_id:id(92)},{}]){const h=harness({create:null,projection});await assert.rejects(h.run(),/CORRECTION_CONTEXT_|CORRECTION_REQUEST_/);assert.ok(!h.log.some(c=>c[0]==='authority'));}
});
test('genuine create P0 and request projection CAS must remain identical before narrow authority read',async()=>{
 const h=harness({projection:{...descriptor,state_revision:2}});await assert.rejects(h.run(),/CORRECTION_REQUEST_CAS_STALE/);assert.ok(!h.log.some(c=>c[0]==='authority'));
});
test('native authority wire contract/decimal bigint CAS/state/P0/return identity stays exact',async()=>{
 for(const value of [{...authority,state_revision:'9007199254740992'},{...authority,state_revision:'2'},{...authority,state_revision:1},{...authority,state:'APPLIED'},
  {...authority,request_id:id(90)},{...authority,replacement_timesheet_id:id(91)},{...authority,extra:true},{...authority,return_invoice_id:'unit-invalid'}])
  await assert.rejects(harness({authority:value}).run(),/CORRECTION_REQUEST_CAS_STALE/);
});
test('actor/view/flags change after native read blocks response including possible admin invoice ID',async()=>{
 for(const user of [{...admin,id:id(93)},pay,{...admin,is_active:false}])await assert.rejects(harness({users:[admin,user]}).run(),/CORRECTION_ACTOR_NOT_ALLOWED/);
 await assert.rejects(harness({users:[pay,{...pay,payment_authoriser:false}]}).run(),/CORRECTION_ACTOR_NOT_ALLOWED/);
});
test('GET wrapper binds fixed current owner RPCs and raw reader transport, with genuine no-store result',async()=>{
 const reverse=JSON.parse(fs.readFileSync(new URL('./BACKEND_CONTEXT_FULL_REVERSE.json',import.meta.url))),h=reverse.hunks[1].replacement;
 const block=h.slice(0,h.lastIndexOf('async function handleTimesheetCorrectionOfficeRoute(')),log=[];
 const f=Function('owners',`const { readTimesheetCorrectionOfficeContext,requireUser,assertTimesheetCorrectionSourceSubjects,rpcTsfinLoadContextBatch,sbRpc,withCORS,JSON_HEADERS,timesheetCorrectionOfficeFailure }=owners;\n${block}\nreturn handleTimesheetCorrectionOfficeContext;`)({
  readTimesheetCorrectionOfficeContext,requireUser:async()=>pay,assertTimesheetCorrectionSourceSubjects:async(_,__,user,ids)=>{log.push(['scope',ids,user.id]);},
  rpcTsfinLoadContextBatch:async()=>harness().owners.readContexts([I.p0]),
  sbRpc:async(_,name,args,options)=>{log.push([name,args,options]);return name==='timesheet_correction_request_authority_v1'?authority:descriptor;},
  withCORS:(_,__,r)=>r,JSON_HEADERS:{'content-type':'application/json'},timesheetCorrectionOfficeFailure:e=>{throw e;}
 });
 const response=await f(env,request(),I.p0);assert.equal(response.status,200);assert.equal(response.headers.get('cache-control'),'no-store');
 assert.deepEqual(log.filter(c=>c[0]!=='scope').map(c=>c[0]),['timesheet_correction_request_projection_v1','timesheet_correction_request_authority_v1']);
 assert.ok(log.filter(c=>c[0]!=='scope').every(c=>c[2].timeoutMs===12000));assert.ok(!JSON.stringify(await response.json()).includes(I.invoice));
});
test('wrong method/query/target is rejected before actor or readers',async()=>{
 for(const [r,id] of [[new Request(request().url,{method:'POST'}),I.p0],[new Request(request().url+'?actor=unit'),I.p0],[request(),'invalid']]){
  const h=harness();await assert.rejects(h.run(env,r,id),e=>e.status===400);assert.equal(h.log.length,0);
 }
});
