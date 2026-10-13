import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {pathToFileURL,fileURLToPath} from 'node:url';
import path from 'node:path';
const root=path.resolve(fileURLToPath(new URL('../../../../',import.meta.url)),'../healthroster-v2-backend-13102026');
// Data-free handler bridge only. No HTTP socket, database, account, deployment,
// signature or phone is used. These tests cannot produce acceptance receipts.
let source=fs.readFileSync(new URL('./17_complete_current_routes.js',import.meta.url),'utf8');
source=source.replace(/from '([^']+)'/g,(_match,relative)=>"from '"+pathToFileURL(path.resolve(root,'broker/src/weekly-source',relative)).href+"'");
const {dispatchWeeklySourceRequest}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
const {orchestrateWeeklyProtectedBulk}=await import(pathToFileURL(path.join(root,'broker/src/weekly-source/protected-bulk-action.mjs')));
const actor='00110011-0011-4011-8011-001100110011',cycle='00220022-0022-4022-8022-002200220022';
const request=payload=>new Request('https://handler-protocol.invalid/api/weekly-source/v1/commands',{
 method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action:'PROTECT_ALL_SIGNED',payload})});
function deps(calls,role='admin'){
 return {requireUser:async()=>({id:actor,is_active:true,role,payment_authoriser:true,payment_golden_key:false}),
  orchestrateProtectedBulk:({request,actor:serverActor})=>orchestrateWeeklyProtectedBulk({request,actorUserId:serverActor.id,
   dependencies:{dataRpc:async(name,args)=>{calls.push({name,args});return {ok:true,mode:'PREVIEW',eligible_count:0,
    qualification:{contract:'WEEKLY_SOURCE_TEST_JOURNEY_PRESENTATION_V1',operational_ready:false}};}}})};
}
test('unchanged authenticated handler injects actual trusted actor; qualification marker remains explicitly non-operational',async()=>{
 const calls=[],response=await dispatchWeeklySourceRequest(request({source_cycle_id:cycle,mode:'PREVIEW'}),{},null,deps(calls));
 assert.equal(response.status,200);assert.equal(calls.length,1);assert.equal(calls[0].name,'weekly_source_protect_all_signed_atomic_v1');
 assert.deepEqual(calls[0].args.p_request,{mode:'PREVIEW',actor_user_id:actor,source_cycle_id:cycle});
 const body=await response.json();assert.equal(body.qualification.operational_ready,false);
});
test('pay authoriser uses the same trusted route admission and native RPC, never a browser role inference',async()=>{
 const calls=[],response=await dispatchWeeklySourceRequest(request({source_cycle_id:cycle,mode:'PREVIEW'}),{},null,deps(calls,'office'));
 assert.equal(response.status,200);assert.equal(calls[0].args.p_request.actor_user_id,actor);
});
test('caller actor, readiness, qualification scope and finance vectors are refused before the native RPC',async()=>{
 for(const patch of [{actor_user_id:cycle},{producer_ready:true},{qualification_scope_id:cycle},{pay_amount:123}]){
  const calls=[],response=await dispatchWeeklySourceRequest(request({source_cycle_id:cycle,mode:'PREVIEW',...patch}),{},null,deps(calls));
  assert.notEqual(response.status,200);assert.equal(calls.length,0);
 }
});
test('missing native composition stays unavailable; handler presence cannot provide READY',async()=>{
 const response=await dispatchWeeklySourceRequest(request({source_cycle_id:cycle,mode:'PREVIEW'}),{},null,
  {requireUser:async()=>({id:actor,is_active:true,role:'admin'})});
 assert.equal(response.status,503);assert.equal((await response.json()).error_code,'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE');
});
