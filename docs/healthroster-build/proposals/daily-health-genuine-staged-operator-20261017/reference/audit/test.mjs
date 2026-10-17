// Actual captured JS functions; finite synthetic transport records only.
// These tests prove routing/refusal/partial-failure facts, not financial/native results.
import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import vm from'node:vm';
import{here,census,recipe,raw,offlineFunction,assertCurrentSource,deriveFixture}from'./source.mjs';
const copy=v=>JSON.parse(JSON.stringify(v)),env={SUPABASE_URL:'https://offline.invalid'},stage=id=>copy(recipe.stages.find(s=>s.id===id).body);
const result=v=>v,withCORS=(_e,_r,v)=>v,ok=v=>({status:200,body:v}),badRequest=v=>({status:400,error:v}),serverError=v=>({status:500,error:v});
const now='2026-10-17T12:00:00.000Z';class FixedDate extends Date{constructor(...args){super(...args.length?args:[now]);}static now(){return new Date(now).getTime();}}
test('exact current saved/committed source and physical root, twenty-two complete functions',()=>{const b=assertCurrentSource();assert.equal(b.completeFunctions,22);for(const f of census.functions)new vm.Script('('+raw(f.name)+')');});
test('actual pure Worker derivation supplies booking scope, never native IDs or snapshot',async()=>{const d=await deriveFixture();assert.match(d.bookingId,/^bk_[0-9a-f]{16}$/);assert.equal(d.schedule.ymd,'2026-10-15');assert.equal(d.schedule.worked_minutes,360);assert.equal(d.weekEndingDate,'2026-10-18');assert.equal(d.jobTitleNorm,'rn');assert.equal(d.canonicalRole,'RN');assert.equal(d.actualFinancialId,null);assert.equal(d.actualTimesheetId,null);});
test('actual S1 wrapper forwards only genuine allowlisted fields and exact five RPC args',async()=>{
 const constants=new vm.Script(fs.readFileSync(here+'/reference/worker/client-create-fields.js.txt','utf8')+'\n({CLIENT_CREATE_FIELDS,CLIENT_SETTINGS_CREATE_FIELDS})').runInNewContext();let call;
 const fn=offlineFunction('createClientWithSettingsAtomic',{...constants,sbRpc:async(...args)=>{call=args;return{};},unwrapRpcJsonb:result});
 const body=stage('S1');await fn(env,{id:'offline-actor'},body);assert.equal(call[1],'client_create_with_settings_v1');assert.deepEqual(Object.keys(call[2]).sort(),['p_actor_user_id','p_client_id','p_client_json','p_now_utc','p_settings_json']);assert.equal(call[2].p_client_id,body.id);assert.deepEqual(copy(call[2].p_settings_json),body.client_settings);assert.equal(call[3].timeoutMs,30000);assert.ok(!('id'in call[2].p_client_json));
 await assert.rejects(fn(env,{id:'offline-actor'},{...body,id:'not-uuid'}),/CLIENT_CREATE_ID_INVALID/);
});
test('actual targeted empty drain reads settings then returns without enqueue/dequeue/worker',async()=>{
 let settings=0;const forbidden=()=>{throw Error('UNEXPECTED_FINANCIAL_CALL');};const fn=offlineFunction('tsfinTargetedDrainNow',{loadSettingsDefaults:async()=>{settings++;return{};},sbRpc:forbidden,fetch:forbidden,runTsfinWorkerOnce:forbidden});
 const r=await fn(env,{timesheetIds:[]});assert.equal(settings,1);assert.equal(r.enqueued,0);assert.equal(r.picked,0);assert.equal(r.done,true);
});
test('actual Client enqueue is scoped to current, uninvoiced TSFIN and empty read causes no POST',async()=>{
 const reads=[],writes=[];let rows=[];const fn=offlineFunction('enqueueTsfinRecomputeForClient',{sbFetch:async(_e,url)=>{reads.push(url);return{rows};},fetch:async(url,args)=>{writes.push({url,args});return{ok:true};},sbHeaders:()=>({}),nowIso:()=>now});
 await fn(env,recipe.plannedClientId);assert.equal(writes.length,0);const q=new URL(reads[0]);assert.equal(q.pathname,'/rest/v1/timesheets_financials');assert.equal(q.searchParams.get('client_id'),'eq.'+recipe.plannedClientId);assert.equal(q.searchParams.get('is_current'),'eq.true');assert.equal(q.searchParams.get('locked_by_invoice_id'),'is.null');
 rows=[{timesheet_id:'synthetic-existing'}];await fn(env,recipe.plannedClientId);assert.equal(writes.length,1);assert.equal(JSON.parse(writes[0].args.body)[0].timesheet_id,'synthetic-existing');assert.equal(JSON.parse(writes[0].args.body)[0].reason,'RATE_CHANGED');
});
test('actual S3 fresh path posts one row and reaches scoped enqueue for the October window',async()=>{
 const reads=[],writes=[],queued=[];const fn=offlineFunction('handleUpsertClientRate',{Date:FixedDate,requireUser:async(_e,_r,roles)=>{assert.deepEqual(copy(roles),['admin']);return{id:'offline'};},parseJSONBody:async()=>stage('S3'),sbFetch:async(_e,url)=>{reads.push(url);return{rows:[]};},fetch:async(url,args)=>{writes.push({url,args});return{ok:true,json:async()=>[{id:'synthetic-rate'}]};},sbHeaders:()=>({}),nowIso:()=>now,enqueueTsfinRecomputeForClient:async(_e,id)=>queued.push(id),withCORS,ok,badRequest,serverError});
 const r=await fn(env,{});assert.equal(r.status,200);assert.equal(reads.length,3);assert.deepEqual(writes.map(r=>r.args.method),['POST']);assert.deepEqual(queued,[recipe.plannedClientId]);assert.equal(JSON.parse(writes[0].args.body).role,'RN');
});
test('actual S3 incumbent path can PATCH an old window before later POST failure',async()=>{
 let n=0;const writes=[];const fn=offlineFunction('handleUpsertClientRate',{Date:FixedDate,requireUser:async()=>({id:'offline'}),parseJSONBody:async()=>stage('S3'),sbFetch:async()=>({rows:++n===2?[{id:'synthetic-incumbent',date_from:'2026-09-01',date_to:null}]:[]}),fetch:async(url,args)=>{writes.push({url,args});return{ok:args.method==='PATCH',text:async()=>'synthetic refusal'};},sbHeaders:()=>({}),nowIso:()=>now,enqueueTsfinRecomputeForClient:()=>{throw Error('UNEXPECTED_ENQUEUE');},withCORS,ok,badRequest,serverError});
 const r=await fn(env,{});assert.equal(r.status,400);assert.deepEqual(writes.map(r=>r.args.method),['PATCH','POST']);assert.equal(JSON.parse(writes[0].args.body).date_to,'2026-09-30');
});
test('actual Worker targeted list chooses only specific dequeue; omitted list is general branch',async()=>{
 let specific=0,batch=0;const fn=offlineFunction('runTsfinWorkerOnce',{rpcTsfinDequeueSpecific:async(_e,args)=>{specific++;assert.deepEqual(copy(args),{timesheetIds:['synthetic-ts'],limit:1});return[];},rpcTsfinDequeueBatchIds:async()=>{batch++;return[];}});
 await fn(env,{limit:1,onlyTimesheetIds:['synthetic-ts']});assert.equal(specific,1);assert.equal(batch,0);await fn(env,{limit:1});assert.equal(batch,1);
});
test('actual recalculation forwards exact target and refuses nominal or wrong completion evidence',async()=>{
 for(const worker of [{fail:0,completed_timesheet_ids:[]},{fail:0,completed_timesheet_ids:['different-ts']},{fail:1,completed_timesheet_ids:['synthetic-ts']}]){
  const calls=[];const fn=offlineFunction('saveAndRecalculateCanonicalDaily',{sbRpc:async(_e,name,args)=>calls.push({name,args:copy(args)}),runTsfinWorkerOnce:async(_e,args)=>{calls.push(copy(args));return worker;},sbGetOne:async()=>({id:'synthetic-readback-only'})});
  await assert.rejects(fn(env,{timesheetId:'synthetic-ts',forceUnprocessed:true}),/DAILY_CANONICAL_RECALCULATION_FAILED/);assert.deepEqual(calls[0],{name:'enqueue_ts_financials_priority',args:{_timesheet_ids:['synthetic-ts'],_reason:'CONTEXT_CHANGED'}});assert.deepEqual(calls[1],{limit:1,onlyTimesheetIds:['synthetic-ts'],forceUnprocessedTimesheetIds:['synthetic-ts']});
 }
});
test('actual S4 failed recalculation plus failed DELETE returns 500 after an INSERT',async()=>{
 const derived=await deriveFixture(),writes=[];let recalc=0;const fn=offlineFunction('handleTimesheetCreateManualDaily',{
  requireUser:async()=>({id:'offline'}),parseJSONBody:async()=>stage('S4'),nowIso:()=>now,
  sbGetOne:async(_e,url)=>url.includes('/candidates?')?{id:recipe.plannedCandidateId,key_norm:stage('S2').key_norm}:{id:recipe.plannedClientId,name:stage('S1').name},
  mapCanonicalDailyScheduleToIso:()=>derived.schedule,ukLocalToUtcISO:()=>{throw Error('UNUSED_NO_FAKE_CALCULATION');},makeBookingId:async()=>derived.bookingId,weekEndingSunday:()=>derived.weekEndingDate,
  crypto:{randomUUID:()=> 'synthetic-idempotency'},fetch:async(url,args)=>{writes.push({url,args});if(args.method==='POST')return{ok:true,json:async()=>[{timesheet_id:'synthetic-created-ts'}]};throw Error('SYNTHETIC_DELETE_FAILURE');},
  saveAndRecalculateCanonicalDaily:async(_e,args)=>{recalc++;assert.deepEqual(copy(args),{timesheetId:'synthetic-created-ts',forceUnprocessed:true});throw Error('SYNTHETIC_RECALC_FAILURE');},sbHeaders:()=>({}),withCORS,ok,badRequest,serverError});
 const r=await fn(env,{headers:{get:()=>null}});assert.equal(r.status,500);assert.equal(recalc,1);assert.deepEqual(writes.map(w=>w.args.method),['POST','DELETE']);assert.equal(JSON.parse(writes[0].args.body)[0].authorised_at_server,null);assert.equal(JSON.parse(writes[0].args.body)[0].contract_id,null);assert.equal(JSON.parse(writes[0].args.body)[0].job_title_norm,'rn');
});
test('native S1 body identity receipt is definition-only and no actor/vacancy/API claim',()=>{
 const proof=JSON.parse(fs.readFileSync(here+'/reference/root-native/GENUINE_CLIENT_NATIVE_SOURCE_IDENTITY_20261017.json'));assert.equal(proof.completeInnerBodyRawEqual,true);assert.equal(proof.SQLNormalisationApplied,false);assert.equal(proof.nativeInvocation,'NOT_RUN');assert.equal(proof.actorVacancyAndSetup,'NOT_BOUND');assert.equal(census.actor,'UNBOUND');
});
