import test from 'node:test';
import assert from 'node:assert/strict';
import {createBpayNextFinancialJobRpc} from '../broker/src/bpay-next-job-rpc.js';
const job='b80f0000-0000-4000-8000-000000000001';
test('fixed job transport forwards one raw bounded call without buffering or redirect',async()=>{
  const signal=new AbortController().signal;
  const raw=new Response('not-yet-consumed');let calls=0;
  const rpc=createBpayNextFinancialJobRpc({baseUrl:'https://agency-test.invalid/',
    headers:{'Content-Type':'application/json',Authorization:'Bearer synthetic-test-only'},fetchImpl:async(url,options)=>{
      calls++;
      assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_job_v1');
      assert.equal(options.signal,signal);assert.equal(options.redirect,'error');
      assert.equal(options.headers.get('content-type'),'application/json');
      assert.equal(options.headers.get('authorization'),'Bearer synthetic-test-only');
      assert.equal(options.headers.get('X-CloudTMS-Banking-Pay-Route-Class'),'OPERATION_NUDGE');
      assert.deepEqual(JSON.parse(options.body),{p_action:'STEP',p_job_id:job,p_job_kind:'CSV_SETTLEMENT',
        p_args:{lease_nonce:job,owner_epoch:'9007199254740993',expected_cursor:null,limit:1}});
      return raw;
    }});
  const response=await rpc('STEP',job,'CSV_SETTLEMENT',{
    lease_nonce:job,owner_epoch:'9007199254740993',expected_cursor:null,limit:1},{signal});
  assert.equal(response,raw);assert.equal(response.bodyUsed,false);assert.equal(calls,1);
});
test('arbitrary job/action, missing signal and credential URL never reach fetch',async()=>{
  assert.throws(()=>createBpayNextFinancialJobRpc({baseUrl:'https://user:password@test.invalid'}),TypeError);
  assert.throws(()=>createBpayNextFinancialJobRpc({baseUrl:'file:///tmp/anything'}),TypeError);
  let calls=0;
  const rpc=createBpayNextFinancialJobRpc({baseUrl:'https://agency-test.invalid',fetchImpl:()=>{calls++;throw Error('must not fetch');}});
  await assert.rejects(rpc('SQL',job,'CSV_RETURN',{}, {signal:new AbortController().signal}),TypeError);
  await assert.rejects(rpc('CLAIM',job,'PREPARE_RUN',{}, {signal:new AbortController().signal}),TypeError);
  await assert.rejects(rpc('CLAIM',job,'CSV_RETURN',{}, {}),TypeError);
  assert.equal(calls,0);
});
test('transport failure is not retried or changed to another RPC',async()=>{
  let calls=0;const failure=new Error('synthetic unavailable');
  const rpc=createBpayNextFinancialJobRpc({baseUrl:'https://agency-test.invalid',fetchImpl:()=>{calls++;throw failure;}});
  await assert.rejects(rpc('CLAIM',job,'CSV_RETURN',{lease_seconds:120},{signal:new AbortController().signal}),failure);
  assert.equal(calls,1);
});
test('POSITION/PREPARE travel only through the same fixed service RPC with exact phase cursor text',async()=>{
  const calls=[];const signal=new AbortController().signal;
  const rpc=createBpayNextFinancialJobRpc({baseUrl:'https://agency-test.invalid',fetchImpl:async(url,options)=>{
    calls.push({url,body:JSON.parse(options.body)});return new Response('unbuffered');
  }});
  const tuples=[['POSITION_APPLY',{lease_nonce:job,owner_epoch:'9007199254740993',expected_phase:'REMOVED',expected_cursor:'retained Ω component',limit:100}],
    ['PREPARE',{lease_nonce:job,owner_epoch:'9007199254740993',expected_phase:'RESERVE',expected_work_cursor:job,expected_component_cursor:'retained Ω component',limit:100}]];
  for(const[kind,args]of tuples) await rpc('STEP',job,kind,args,{signal});
  assert.equal(calls.length,2);
  for(let i=0;i<calls.length;i++){
    assert.equal(calls[i].url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_job_v1');
    assert.deepEqual(calls[i].body,{p_action:'STEP',p_job_id:job,p_job_kind:tuples[i][0],p_args:tuples[i][1]});
  }
});

test('internal settlement forwards a single exact page through the same fixed job RPC',async()=>{
  let calls=0;
  const signal=new AbortController().signal;
  const args={lease_nonce:job,owner_epoch:'9007199254740993',expected_cursor:null,limit:17};
  const raw=new Response('unbuffered');
  const rpc=createBpayNextFinancialJobRpc({baseUrl:'https://agency-test.invalid',fetchImpl:async(url,options)=>{
    calls++;
    assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_job_v1');
    assert.deepEqual(JSON.parse(options.body),{p_action:'STEP',p_job_id:job,p_job_kind:'INTERNAL_SETTLEMENT',p_args:args});
    assert.equal(options.signal,signal);
    return raw;
  }});
  assert.equal(await rpc('STEP',job,'INTERNAL_SETTLEMENT',args,{signal}),raw);
  assert.equal(raw.bodyUsed,false);assert.equal(calls,1);
});

test('CASE_CREATE claim and lease-only step use only the fixed unbuffered service RPC',async()=>{
  const calls=[];const signal=new AbortController().signal;
  const rpc=createBpayNextFinancialJobRpc({baseUrl:'https://agency-test.invalid',fetchImpl:async(url,options)=>{
    const raw=new Response('unbuffered');calls.push({url,options,raw});return raw;
  }});
  for(const[action,args]of [['CLAIM',{lease_seconds:120}],['STEP',{lease_nonce:job,owner_epoch:'9007199254740993'}]]){
    const raw=await rpc(action,job,'CASE_CREATE',args,{signal});
    const call=calls.at(-1);
    assert.equal(call.url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_job_v1');
    assert.deepEqual(JSON.parse(call.options.body),{p_action:action,p_job_id:job,p_job_kind:'CASE_CREATE',p_args:args});
    assert.equal(call.options.signal,signal);assert.equal(call.options.redirect,'error');
    assert.equal(raw,call.raw);assert.equal(raw.bodyUsed,false);
  }
  assert.equal(calls.length,2);
});
