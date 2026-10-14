import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';import {execFileSync} from 'node:child_process';import vm from 'node:vm';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),mount=path.join(dir,'.merge-inputs','test-mount');
const read=file=>fs.readFileSync(path.join(dir,'forward',file),'utf8'),git=(commit,file)=>execFileSync('git',['show',commit+':'+file],{cwd:own,encoding:'utf8',maxBuffer:64*1024*1024});
const current='37ddadc0a50721834c9e8a6c06becfc983f0dc32',pins=JSON.parse(fs.readFileSync(path.join(dir,'THREE_WAY_INPUTS.json')));
const moduleAt=file=>import(pathToFileURL(path.join(mount,file)).href);
const {createOffRuleTransportRpc}=await moduleAt('broker/src/weekly-source/off-rule-transport-rpc.mjs');
const {isActivePaymentActor,requireActivePaymentActor}=await moduleAt('broker/src/payment-actor-permission.js');
const {candidateBrokerInternals:broker}=await moduleAt('candidate-broker/src/candidate-broker.js');
const {validateLegacyInvoiceManifestSuppressions}=await moduleAt('broker/src/weekly-source/invoice-render-qualification.js');
const id='10000000-0000-4000-8000-000000000001';
test('UNIT_ONLY: all four complete forward/reverse transformations retain current bytes outside exact hunks',()=>{
 const proof=JSON.parse(fs.readFileSync(path.join(dir,'FULL_RUNTIME_REVERSE.json')));assert.equal(proof.files.length,4);
 for(const row of proof.files){const lines=read(row.file).match(/[^\n]*\n|[^\n]+$/g)??[];
  for(const h of [...row.hunks].reverse())lines.splice(h.new_line_start,h.new_line_count,...(h.before.match(/[^\n]*\n|[^\n]+$/g)??[]));
  assert.equal(lines.join(''),git(current,row.file));}
});
test('UNIT_ONLY: current Later Change Decision overlap is retained in full, including native refusal/order semantics',()=>{
 const chosen=pins.resolutions.find(row=>row.reason.includes('Later Change'));assert.ok(chosen);assert.equal(chosen.selected,chosen.current);
 assert.ok(read(chosen.file).includes(chosen.current));assert.ok(chosen.current.includes('LATER_CHANGE_DECISION'));
});
test('UNIT_ONLY: Money adapter is exactly two source hunks with two direct/five nested slots and one trusted actor callback',()=>{
 const text=read('broker/src/index.js'),h=pins.money_adapter.hunks[1];assert.ok(text.includes(h.after));
 assert.equal((h.after.match(/rpc: weeklySourceOffRpc/g)??[]).length,2);assert.equal((h.after.match(/dataRpc: weeklySourceOffRecordingRpc/g)??[]).length,5);
 assert.equal((h.after.match(/weeklySourceAuthenticatedActorId = actor\?\.is_active === true \? actor.id : null/g)??[]).length,1);
 for(const marker of ['c1RawRpc: createWeeklySourceC1RawRpc({','buildOrdinaryServiceSnapshot: (calculationInput) =>','calculateWeeklySnapshot: (calculationInput) =>'])
  assert.equal(h.after.split(marker).length,h.before.split(marker).length);
});
test('UNIT_ONLY: non-TEST adapter is identity and ordinary/native C1 args/options/errors preserve raw object identity',async()=>{
 const failure=Error('UNIT_ONLY_NATIVE_FAILURE'),calls=[],args={p_request:{opaque:['UNIT_ONLY']}},options={timeoutMs:20000};
 const base=async(...values)=>{calls.push(values);throw failure;};assert.equal(createOffRuleTransportRpc({baseRpc:base,getActor:()=>null,agencyId:id,environment:'LIVE'}),base);
 const rpc=createOffRuleTransportRpc({baseRpc:base,getActor:()=>null,agencyId:id,environment:'TEST'});
 await assert.rejects(rpc('ordinary_c1_native_raw_owner',args,options),e=>e===failure);assert.equal(calls[0][1],args);assert.equal(calls[0][2],options);
});
test('UNIT_ONLY: unchanged no-descriptor caller delegates the original native operation after measured not-qualified status',async()=>{
 const args={p_request:{source_cycle_id:id}},options={signal:'UNIT_ONLY'},calls=[],answer={native:'UNIT_ONLY'};
 const rpc=createOffRuleTransportRpc({getActor:()=>id,agencyId:id,environment:'TEST',baseRpc:async(name,a,o)=>{
  calls.push({name,a,o});return calls.length===1?{contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:false,operational_ready:false}:answer;
 }});
 assert.equal(await rpc('weekly_source_finalise_atomic_v1',args,options),answer);
 assert.equal(calls.length,2);assert.equal(calls[1].a,args);assert.equal(calls[1].o,options);
});
test('UNIT_ONLY: real current payment flags require an active actor and preserve native permission dependency errors',async()=>{
 for(const flag of ['payment_authoriser','payment_golden_key']){assert.equal(isActivePaymentActor({is_active:true,role:'office',[flag]:true}),true);
  assert.equal(isActivePaymentActor({is_active:false,role:'admin',[flag]:true}),false);}
 const denied=await requireActivePaymentActor({},id,{readUser:async()=>{throw Error('UNIT_ONLY');}});assert.equal(denied.response.status,500);
 const permitted=await requireActivePaymentActor({},id,{readUser:async()=>({id,is_active:true,role:'office',payment_golden_key:true})});assert.equal(permitted.ok,true);
});
test('UNIT_ONLY: Candidate operation/native data-plane registry boundaries remain exact current Git objects',()=>{
 const rows=JSON.parse(fs.readFileSync(path.join(dir,'RUNTIME_DEPENDENCY_PINS.json'))).files;
 const targets=rows.filter(row=>/candidate-operation-policy|candidate-data-plane.*registry|candidate-rpc.*registry/.test(row.file));assert.ok(targets.length>=2);
 for(const row of targets)assert.equal(fs.readFileSync(path.join(mount,row.file),'utf8'),git(current,row.file));
 const dataRows=rows.filter(row=>/candidate-data-plane.*registry/.test(row.file));assert.ok(dataRows.length);
 assert.ok(dataRows.every(row=>row.sha256.length===64));
});
test('UNIT_ONLY: public Candidate optional correction validation preserves exact ordinary bytes/headers and refuses undeclared facts',async()=>{
 const request=new Request('https://unit-only.invalid/timesheets'),operation={operation_id:'getCandidateTimesheet'},bytes='{ "timesheet" : null }\n';
 const response=new Response(bytes,{headers:{'content-type':'application/json','x-unit-only':'retained'}});
 assert.equal(await broker.correctionPrivateResponse(response,operation,request),response);assert.equal(await response.text(),bytes);
 await assert.rejects(broker.correctionPrivateResponse(Response.json({timesheet:{id},correction:{state:'WAITING',caller_pass:true}}),operation,request),e=>e.code==='CANDIDATE_PRIVATE_API_UNAVAILABLE');
});
test('UNIT_ONLY: native legacy render consumer rejects invented ROUND suppression instead of silently dropping a line',()=>{
 const ordinary={invoice:{id},lines:[{id}]};assert.equal(validateLegacyInvoiceManifestSuppressions(ordinary),ordinary);
 assert.throws(()=>validateLegacyInvoiceManifestSuppressions({...ordinary,source_rounding_suppressions:[{caller_assertion:true}]}));
});
test('UNIT_ONLY: actual Candidate correction projection retains selected-candidate/request/purpose boundaries',async()=>{
 const source=read('broker/src/candidate-app-backend.js'),start=source.indexOf('async function enrichCandidateCorrection('),end=source.indexOf('\nasync function handleCandidateRead(',start);
 assert.ok(start>=0&&end>start);const calls=[],detail={timesheet:{id,sheet_scope:'WEEKLY'}},raw={root_timesheet_id:id,reversed_timesheet_id:id,replacement_timesheet_id:null};
 const context=vm.createContext({correctionProjection:async(...args)=>{calls.push(args);return raw;},admitCorrectionReplacement:()=>true,
  serializeCandidateCorrection:()=>({UNIT_ONLY:'purpose-closed'}),CORRECTION_CONVERTERS:{},frozenCorrectionDailyShift:()=>{throw Error('UNEXPECTED_DAILY');}});
 const fn=new vm.Script(source.slice(start,end)+'\nenrichCandidateCorrection').runInContext(context),request={UNIT_ONLY:true},deps={},access={selected_candidate_id:id};
 const output=await fn(request,deps,access,detail);assert.equal(output.timesheet,detail.timesheet);assert.equal(calls[0][0],deps);assert.equal(calls[0][1],id);assert.equal(calls[0][2],id);
});
