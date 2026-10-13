import test from 'node:test';
import assert from 'node:assert/strict';
import {createBpayNextPreparationRpc,createBpayNextPreparationPageRpc} from '../broker/src/bpay-next-preparation-rpc.js';
const actor='b8570000-0000-4000-8000-000000000001';
const run='b8570000-0000-4000-8000-000000000002';
for(const page of [false,true])test(`fixed ${page?'page':'intake'} transport forwards one raw response with service actor and redirect refusal`,async()=>{
  let calls=0;const signal=new AbortController().signal;const raw=new Response('not-yet-buffered');
  const name=page?'WORK_CHOICES':'SEAL_ENQUEUE';const args=page?{run_id:run,after_work_id:null,limit:1}:{run_id:run,command_id:run,expected_pages:'0',expected_count:'0'};
  const rpc=(page?createBpayNextPreparationPageRpc:createBpayNextPreparationRpc)({baseUrl:'https://agency-test.invalid/',
    headers:{Authorization:'Bearer synthetic-test-only'},fetchImpl:async(url,options)=>{
      calls++;assert.equal(url,`https://agency-test.invalid/rest/v1/rpc/bpay_next_preparation_${page?'page':'intake'}_v1`);
      assert.equal(options.method,'POST');assert.equal(options.redirect,'error');assert.equal(options.signal,signal);
      assert.equal(options.headers.get('authorization'),'Bearer synthetic-test-only');assert.equal(options.headers.get('content-type'),'application/json');
      assert.equal(options.headers.get('X-CloudTMS-Banking-Pay-Route-Class'),page?'READ':'OPERATION_NUDGE');
      assert.deepEqual(JSON.parse(options.body),{p_actor_user_id:actor,[page?'p_kind':'p_action']:name,p_args:args});return raw;
    }});
  assert.equal(await rpc(name,args,{actorUserId:actor,signal}),raw);assert.equal(raw.bodyUsed,false);assert.equal(calls,1);
});
test('arbitrary SQL/function/action, credentials in URL and missing dependency signal cannot fetch',async()=>{
  for(const create of [createBpayNextPreparationRpc,createBpayNextPreparationPageRpc]){
    assert.throws(()=>create({baseUrl:'https://user:password@test.invalid'}),TypeError);assert.throws(()=>create({baseUrl:'file:///arbitrary'}),TypeError);
    let calls=0;const rpc=create({baseUrl:'https://test.invalid',fetchImpl:()=>{calls++;throw Error('no fetch');}});
    for(const[name,options]of [['SQL',{actorUserId:actor,signal:new AbortController().signal}],['WORK_PAGE',{}],['private.owner',{actorUserId:actor,signal:new AbortController().signal}]])
      await assert.rejects(rpc(name,{},options),TypeError);
    assert.equal(calls,0);
  }
});
test('transport failure is retained exactly with no retry/fallback or response JSON parsing',async()=>{
  const error=new Error('synthetic transport failure');let calls=0;
  const rpc=createBpayNextPreparationRpc({baseUrl:'https://test.invalid',fetchImpl:()=>{calls++;throw error;}});
  await assert.rejects(rpc('REVIEW',{run_id:run},{actorUserId:actor,signal:new AbortController().signal}),error);assert.equal(calls,1);
});
test('CURRENT_WORK and STATUS use only the same fixed page wrapper with explicit decimal-text composite cursor inputs',async()=>{
 const signal=new AbortController().signal;const calls=[];
 const rpc=createBpayNextPreparationPageRpc({baseUrl:'https://test.invalid',fetchImpl:async(url,options)=>{
  calls.push({url,body:JSON.parse(options.body)});return new Response('unbuffered');}});
 const inputs=[['CURRENT_WORK',{candidate_id:actor,after_week:'2026-09-27',after_work_id:run,limit:100}],
  ['STATUS',{run_id:run,run_worker_id:actor,after_transfer_no:'2147483647',limit:1}]];
 for(const[kind,args]of inputs)await rpc(kind,args,{actorUserId:actor,signal});
 assert.equal(calls.length,2);for(let n=0;n<2;n++){
  assert.equal(calls[n].url,'https://test.invalid/rest/v1/rpc/bpay_next_preparation_page_v1');
  assert.deepEqual(calls[n].body,{p_actor_user_id:actor,p_kind:inputs[n][0],p_args:inputs[n][1]});
 }
});
