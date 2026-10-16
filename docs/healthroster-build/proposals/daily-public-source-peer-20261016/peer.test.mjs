import test from 'node:test';import assert from 'node:assert/strict';
import {handleDailyRateCorrectionAccept,handleDailyRateCorrectionGet} from './frozen/existing-exact/broker/src/daily-rate-correction.js';
const id=n=>'10000000-0000-4000-8000-'+String(n).padStart(12,'0'),version='DAILY_RATE_CORRECTION_V1';
const body=()=>({version,request_id:id(1),operation_id:id(2),review_id:id(3),review_digest:'a'.repeat(64),expected_state_revision:'9007199254740993'});
const request=value=>new Request('https://fixture.invalid/api/timesheets/'+id(4)+'/daily-rate-correction/accept',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(value)});
const get=()=>new Request('https://fixture.invalid/api/timesheets/daily-rate-correction/operations/'+id(2));
const actor=role=>async()=>({id:id(5),is_active:true,role});
test('current role loss refuses Accept before any native call, including a saved body',async()=>{
 let calls=0;const response=await handleDailyRateCorrectionAccept(request(body()),{},{requireUser:actor('payment_authoriser'),rpc:async()=>{calls++;throw Error('must not call');}});
 assert.equal(calls,0);assert.equal(response.status,403);assert.deepEqual(await response.json(),{ok:false,error_code:'DAILY_RATE_ACTOR_NOT_ALLOWED',retryable:false});
});
test('GET uses the fresh reader actor and returns no content without the current compositor',async()=>{
 let calls=0;const response=await handleDailyRateCorrectionGet(get(),{},{requireUser:actor('payment_authoriser'),rpc:async(name,args)=>{calls++;assert.equal(name,'timesheet_daily_rate_correction_result_v1');assert.deepEqual(args,{p_args:{version,operation_id:id(2),actor_user_id:id(5)}});return Response.json({code:'55000',message:'SENSITIVE_SAVED_RECEIPT_MUST_NOT_LEAVE'},{status:500});}});
 assert.equal(calls,1);assert.equal(response.status,503);assert.deepEqual(await response.json(),{ok:false,error_code:'DAILY_RATE_NATIVE_DEPENDENCY_UNAVAILABLE',retryable:true});
});
test('accepted request with missing result dependency stays uncertain without returning saved content',async()=>{
 const response=await handleDailyRateCorrectionAccept(request(body()),{},{requireUser:actor('admin'),rpc:async()=>Response.json({code:'55000',message:'SENSITIVE_SAVED_RECEIPT_MUST_NOT_LEAVE'},{status:500})});
 assert.equal(response.status,503);assert.deepEqual(await response.json(),{ok:false,error_code:'DAILY_RATE_RESULT_UNCERTAIN',retryable:true});
});
test('current native read denial is returned without old receipt or arbitrary native error text',async()=>{
 const response=await handleDailyRateCorrectionGet(get(),{},{requireUser:actor('admin'),rpc:async()=>Response.json({code:'42501',message:'SENSITIVE_PREVIOUS_FULL_RESULT'},{status:403})});
 assert.equal(response.status,403);assert.deepEqual(await response.json(),{ok:false,error_code:'DAILY_RATE_ACTOR_NOT_ALLOWED',retryable:false});
});
test('browser actor spoof and extra GET query refuse before any RPC',async()=>{
 let calls=0;const deps={requireUser:actor('admin'),rpc:async()=>{calls++;throw Error('must not call');}};
 assert.equal((await handleDailyRateCorrectionAccept(request({...body(),actor_user_id:id(8)}),{},deps)).status,400);
 assert.equal((await handleDailyRateCorrectionGet(new Request(get().url+'?actor_user_id='+id(8)),{},deps)).status,400);assert.equal(calls,0);
});
test('private Source native outer result can never become the Daily public result',async()=>{
 const privateValue={version:'SOURCE_NATIVE_COMPOSED_RESULT_V1',route:'SOURCE_LOCAL',native_result_json:{private:'SENSITIVE'}};
 const response=await handleDailyRateCorrectionGet(get(),{},{requireUser:actor('admin'),rpc:async()=>Response.json(privateValue)});
 assert.equal(response.status,502);assert.deepEqual(await response.json(),{ok:false,error_code:'DAILY_RATE_RESPONSE_INVALID',retryable:false});
});
test('inactive authenticated user refuses GET before any native read',async()=>{
 let calls=0;const response=await handleDailyRateCorrectionGet(get(),{},{requireUser:async()=>({id:id(5),role:'admin',is_active:false}),rpc:async()=>{calls++;}});assert.equal(response.status,401);assert.equal(calls,0);
});
