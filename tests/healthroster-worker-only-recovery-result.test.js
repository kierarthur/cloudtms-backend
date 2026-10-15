// Exercise the actual canonical recovery owner with effect-free dependencies.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import assert from 'node:assert/strict';import test from 'node:test';import {fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const backend=fs.readFileSync(path.join(root,'broker/src/candidate-app-backend.js'),'utf8');
const name='export async function recoverPendingCandidateManagerFinalisations(';
function actual(s){const start=s.indexOf(name),end=s.indexOf('\nexport ',start+name.length);assert.ok(start>=0&&end>start);return s.slice(start,end).replace(/^export /,'');}
const id='00000000-0000-4000-8000-000000000101';
const worker={id,generation:3,scope:'WEEKLY',workflow_kind:'CONTRACT_HOURS',route:'ELECTRONIC',state:'READY_TO_FINALISE',policy_snapshot_json:{manager_approval_policy:{approval:'NOT_REQUIRED'}}};
const manager={...worker,route:'PHONE',policy_snapshot_json:{manager_approval_policy:{}}};
function harness(options={}){
 const {workflow=worker,leaseDeferred=false,target=false,finaliseError=null,source=backend,rows=[{id,generation:3}]}=options;
 const result=Object.hasOwn(options,'result')?options.result:{ok:true};
 const calls=[];let leased=false;
 class CandidateHttpError extends Error{constructor(status,code){super(code);this.status=status;this.code=code;}}
 const context={Math,Number,Date,encodeURIComponent,parseJson:v=>v,environmentName:()=>'TEST',
  requireUuid:v=>v,requireInteger:v=>v,CandidateHttpError,MANAGER_FINALISATION_RPC_TIMEOUT_MS:20000,
  restRows:async(...args)=>{calls.push({kind:'rows',args});return rows;},
  withManagerFinalisationLease:async(_env,rowId,generation,fn)=>{assert.equal(leased,false,'Sequential retained leases');calls.push({kind:'lease',rowId,generation});if(leaseDeferred)return{single_flight_deferred:true};leased=true;try{return await fn();}finally{leased=false;calls.push({kind:'release'});}},
  workflowRow:async()=>{assert.equal(leased,true);calls.push({kind:'workflow'});return workflow;},
  rpcCall:async(_deps,rpc,payload,options)=>{assert.equal(leased,true);calls.push({kind:'rpc',rpc,payload,options});return{final_render_contract:{actual:true}};},
  renderAndRegister:async(...args)=>{assert.equal(leased,true);calls.push({kind:'render',args});},
  finaliseWorkflow:async(...args)=>{assert.equal(leased,true);calls.push({kind:'finalise',args});if(finaliseError)throw Object.assign(new Error(finaliseError),{code:finaliseError});return result;},
  knownErrorCode:v=>v?.code||'',safeCandidateTransportDiagnostic:e=>({code:e.code}),console:{error(...args){calls.push({kind:'error',args});}}};
 const run=vm.runInNewContext('('+actual(source)+')',context);
 return{calls,run:()=>run({}, {},5,target?{target:{workflow_id:id,generation:3}}:{})};
}
for(const [name,result]of [['false',{ok:false,error_code:'CANDIDATE_DOCUMENT_NOT_READY'}],['missing ok',{}],['null',null],['undefined',undefined],['string','OK'],['numeric ok',{ok:1}],['false deferred',{ok:false,single_flight_deferred:false}]]){
 for(const [route,workflow]of [['worker-only',worker],['PHONE manager',manager]])test(`ACTUAL_JS_UNIT ${route} ${name} cannot count as recovered`,async()=>{
  const h=harness({workflow,result}),r=await h.run();assert.equal(r.recovered,0);assert.equal(r.failed,1);assert.equal(r.deferred,0);assert.equal(h.calls.filter(c=>c.kind==='error').length,1);
 });
}
for(const [route,workflow]of [['worker-only',worker],['PHONE manager',manager],['EMAIL manager',{...manager,route:'EMAIL'}]]){
 test(`ACTUAL_JS_UNIT ${route} genuine ok=true counts only after its existing leased path`,async()=>{
  const h=harness({workflow,result:{ok:true}}),r=await h.run();assert.equal(r.recovered,1);assert.equal(r.failed,0);
  assert.equal(h.calls.filter(c=>c.kind==='finalise').length,1);assert.equal(h.calls.filter(c=>c.kind==='rpc').length,route==='worker-only'?0:1);
  assert.equal(h.calls.filter(c=>c.kind==='render').length,route==='worker-only'?0:1);
  for(const c of h.calls.filter(c=>c.kind==='rpc')){assert.equal(c.rpc,'candidate_manager_finalisation_recovery_v1');assert.equal(c.options.timeoutMs,20000);}
 });
 test(`ACTUAL_JS_UNIT ${route} native single-flight deferral precedes completion check`,async()=>{
  const h=harness({workflow,result:{ok:false,single_flight_deferred:true}}),r=await h.run();assert.equal(r.deferred,1);assert.equal(r.recovered,0);assert.equal(r.failed,0);
 });
}
test('ACTUAL_JS_UNIT busy per-workflow lease defers without invoking finalisation or rendering',async()=>{
 const h=harness({leaseDeferred:true}),r=await h.run();assert.equal(r.deferred,1);assert.equal(r.recovered,0);assert.equal(r.failed,0);assert.equal(h.calls.some(c=>['workflow','rpc','render','finalise'].includes(c.kind)),false);
});
test('ACTUAL_JS_UNIT typed target stale generation retains existing skipped semantics',async()=>{
 const h=harness({target:true,finaliseError:'WORKFLOW_GENERATION_CONFLICT'}),r=await h.run();assert.equal(r.skipped,1);assert.equal(r.failed,0);assert.equal(r.recovered,0);
});
test('ACTUAL_JS_UNIT multiple recovery rows retain sequential leases and exact recovery identities',async()=>{
 const h=harness({rows:[{id,generation:3},{id:'00000000-0000-4000-8000-000000000102',generation:3}]}),r=await h.run();assert.equal(r.scanned,2);assert.equal(r.recovered,2);assert.equal(h.calls.filter(c=>c.kind==='release').length,2);
 for(const c of h.calls.filter(c=>c.kind==='finalise'))assert.match(c.args[4],/^candidate-system-worker-only-recovery:/);
});
