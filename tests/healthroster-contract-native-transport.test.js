// Actual mounted Contract transport regression cases from reviewed 41b9004e1290b6c5a46a92cb972427d3b31dfb4d.
import test from "node:test";import assert from "node:assert/strict";
import {createContractPresetWriteTransport} from "../broker/src/weekly-source/contract-preset-write.mjs";
const ID='00000000-0000-0000-0000-000000000001',ACTOR='00000000-0000-0000-0000-000000000002',SOURCE='00000000-0000-0000-0000-000000000003';
const VERSION='2026-10-15T11:22:33.123Z',NEXT='2026-10-15T11:23:00.000Z',env={SUPABASE_URL:'https://agency-test.invalid'};
const row={id:ID,updated_at:NEXT,candidate_manager_approval_policy_json:{approved_emails:['manager@example.invalid'],approval:'NOT_REQUIRED'}};
function fixture({result=row,error=null}={}){const calls=[],transport=createContractPresetWriteTransport({sbRpc:async(...args)=>{calls.push(args);if(error)throw error;return result;},unwrapRpcJsonb:value=>value});return{calls,transport};}
const postOptions=values=>({method:'POST',body:JSON.stringify(values)}),patchOptions=values=>({method:'PATCH',body:JSON.stringify(values)});
const url=env.SUPABASE_URL+'/rest/v1/contracts',patchUrl=url+'?id=eq.'+ID;
test('ACTUAL_TRANSPORT dependency validation refuses incomplete factory',()=>{assert.throws(()=>createContractPresetWriteTransport({sbRpc(){}}),/DEPENDENCY_REQUIRED/);});
test('ACTUAL_TRANSPORT POST uses exact actor, unchanged ordinary values and 20-second RPC and returns actual full native row',async()=>{
 const f=fixture(),values={client_id:SOURCE,start_date:'2026-10-20',end_date:'2026-10-30',rates_json:{paye_day:12.5},weekly_timesheet_source:'HEALTHROSTER'};
 const r=await f.transport(env,ACTOR,url,postOptions(values));assert.equal(r.status,201);assert.deepEqual(await r.json(),[row]);assert.equal(f.calls.length,1);assert.equal(f.calls[0][1],'weekly_source_contract_write_atomic_v1');assert.deepEqual(f.calls[0][2],{p_request:{actor_user_id:ACTOR,mode:'POST',values}});assert.deepEqual(f.calls[0][3],{timeoutMs:20000});
});
test('ACTUAL_TRANSPORT PATCH carries original same-read CAS without a fresh read',async()=>{
 const f=fixture(),r=await f.transport(env,ACTOR,patchUrl,patchOptions({end_date:'2026-10-30',updated_at:NEXT}),{expectedUpdatedAt:VERSION});assert.equal(r.status,200);assert.deepEqual(await r.json(),[row]);assert.equal(f.calls[0][2].p_request.expected_updated_at,VERSION);assert.equal(f.calls[0][2].p_request.contract_id,ID);
});
test('ACTUAL_TRANSPORT actual printed-policy URL CAS retained exactly',async()=>{
 const f=fixture(),r=await f.transport(env,ACTOR,patchUrl+'&updated_at=eq.'+encodeURIComponent(VERSION),patchOptions({candidate_paper_submission_enabled_override:true}),{expectedUpdatedAt:'other'});assert.equal(r.status,200);assert.equal(f.calls[0][2].p_request.expected_updated_at,VERSION);
});
test('ACTUAL_TRANSPORT duplicate carries native source identity/version and no caller policy object',async()=>{
 const f=fixture(),original={client_id:SOURCE,candidate_manager_approval_policy_json:{approval:'NOT_REQUIRED',approved_emails:['forged@example.invalid']},start_date:'2026-10-20'};
 const r=await f.transport(env,ACTOR,url,postOptions(original),{policySource:{id:SOURCE,updated_at:VERSION}});assert.equal(r.status,201);const p=f.calls[0][2].p_request;assert.equal(p.policy_source_contract_id,SOURCE);assert.equal(p.policy_source_expected_updated_at,VERSION);assert.ok(!Object.hasOwn(p.values,'candidate_manager_approval_policy_json'));assert.ok(Object.hasOwn(original,'candidate_manager_approval_policy_json'));
});
const invalid=[
 ['foreign origin',ACTOR,'https://foreign.invalid/rest/v1/contracts',postOptions({client_id:SOURCE}),{},400],
 ['foreign table',ACTOR,env.SUPABASE_URL+'/rest/v1/clients',postOptions({client_id:SOURCE}),{},400],
 ['actor absent','',url,postOptions({client_id:SOURCE}),{},400],
 ['DELETE',ACTOR,patchUrl,{method:'DELETE',body:'{}'},{},400],
 ['array payload',ACTOR,url,postOptions([{client_id:SOURCE}]),{},400],
 ['null payload',ACTOR,url,postOptions(null),{},400],
 ['empty payload',ACTOR,url,postOptions({}),{},400],
 ['malformed JSON',ACTOR,url,{method:'POST',body:'{'},{},400],
 ['oversized payload',ACTOR,url,{method:'POST',body:JSON.stringify({x:'a'.repeat(262144)})},{},400],
 ['ordinary caller approval',ACTOR,url,postOptions({candidate_manager_approval_policy_json:{approval:'NOT_REQUIRED'}}),{},400],
 ['duplicate source incomplete',ACTOR,url,postOptions({client_id:SOURCE}),{policySource:{id:SOURCE}},400],
 ['duplicate source nonstring version',ACTOR,url,postOptions({client_id:SOURCE}),{policySource:{id:SOURCE,updated_at:123}},400],
 ['PATCH missing original version',ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{},409],
 ['PATCH nonstring version',ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:1},400],
 ['PATCH multiple ids',ACTOR,patchUrl+'&id=eq.'+SOURCE,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:VERSION},400],
 ['PATCH multiple versions',ACTOR,patchUrl+'&updated_at=eq.a&updated_at=eq.b',patchOptions({end_date:'2026-10-30'}),{},400],
 ['PATCH non-equality',ACTOR,patchUrl+'&updated_at=gt.a',patchOptions({end_date:'2026-10-30'}),{},400],
 ['PATCH policy source',ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{policySource:{id:SOURCE,updated_at:VERSION}},400],
 ['unknown query filter',ACTOR,patchUrl+'&client_id=eq.'+SOURCE,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:VERSION},400],
 ['POST id filter',ACTOR,patchUrl,postOptions({client_id:SOURCE}),{},400]
];
for(const [name,actor,target,options,context,status]of invalid)test('ACTUAL_TRANSPORT rejects '+name,async()=>{const f=fixture(),r=await f.transport(env,actor,target,options,context);assert.equal(r.status,status);assert.equal(f.calls.length,0);assert.match((await r.json()).code,/^CANDIDATE_[A-Z0-9_]+$/);});
for(const [name,error,code,status]of [
 ['stale native CAS',{json:{message:'CANDIDATE_FIXED_APPROVAL_CONTRACT_CONTEXT_STALE'}},'CANDIDATE_FIXED_APPROVAL_CONTRACT_CONTEXT_STALE',409],
 ['native admin denial',{message:'WEEKLY_SOURCE_ADMIN_REQUIRED'},'WEEKLY_SOURCE_ADMIN_REQUIRED',403],
 ['native invalid values',{message:'CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID'},'CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID',400],
 ['current RPC cache miss',{json:{code:'PGRST202',message:'routine is absent'}},'CANDIDATE_FIXED_APPROVAL_CONTRACT_DEPENDENCY_UNAVAILABLE',503],
 ['closed unknown failure',{message:'internal private payload must not escape'},'CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_FAILED',503]
])test('ACTUAL_TRANSPORT maps '+name,async()=>{const f=fixture({error}),r=await f.transport(env,ACTOR,url,postOptions({client_id:SOURCE}));assert.equal(r.status,status);assert.deepEqual(await r.json(),{code});assert.equal(f.calls.length,1);});
for(const result of [null,false,[],{}, {id:SOURCE}, {id:'invalid'}])test('ACTUAL_TRANSPORT malformed/foreign native result '+JSON.stringify(result),async()=>{const f=fixture({result}),r=await f.transport(env,ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:VERSION});assert.equal(r.status,503);assert.deepEqual(await r.json(),{code:'CANDIDATE_FIXED_APPROVAL_CONTRACT_RESULT_INVALID'});});
