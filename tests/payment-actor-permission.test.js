import assert from 'node:assert/strict';
import test from 'node:test';
import { isActivePaymentActor, requireActivePaymentActor } from '../broker/src/payment-actor-permission.js';

const id='f7100000-0000-4000-8000-000000000001';
const active={id,is_active:true,role:'user',payment_authoriser:false,payment_golden_key:false};

test('WP-PP has one strict active admin OR authoriser OR golden-key predicate',()=>{
  for(const actor of [{...active,role:'admin'},{...active,payment_authoriser:true},{...active,payment_golden_key:true},
    {...active,role:' ADMIN '}]){
    assert.equal(isActivePaymentActor(actor),true);
    assert.equal(isActivePaymentActor({...actor,is_active:false}),false);
    assert.equal(isActivePaymentActor({...actor,is_active:'true'}),false);
  }
  for(const actor of [null,{},active,{...active,payment_authoriser:'true'},{...active,payment_golden_key:1}]){
    assert.equal(isActivePaymentActor(actor),false);
  }
});

test('current-user transport keeps missing/inactive 401, flags 403, and non-sensitive dependency 500',async()=>{
  for(const [row,status,code] of [[null,401,'Unauthorized'],[{...active,is_active:false},401,'Unauthorized'],
    [active,403,'PAYMENT_PERMISSION_REQUIRED'],[{...active,id:'f7100000-0000-4000-8000-000000000099',role:'admin'},401,'Unauthorized']]){
    const result=await requireActivePaymentActor({},id,{readUser:async()=>row});
    assert.equal(result.ok,false); assert.equal(result.response.status,status);
    assert.deepEqual(await result.response.json(),{error:code});
  }
  const failed=await requireActivePaymentActor({},id,{readUser:async()=>{throw new Error('sensitive upstream body');}});
  assert.equal(failed.response.status,500);
  assert.doesNotMatch(await failed.response.text(),/sensitive|upstream/);
  const absent=await requireActivePaymentActor({},'',{readUser:async()=>assert.fail('invalid IDs are not fetched')});
  assert.equal(absent.response.status,401);
});

test('server transport adapter receives only the exact actor and minimal field set; response hooks keep caller shape',async()=>{
  const calls=[];
  const result=await requireActivePaymentActor({},id,{readUser:async(...args)=>{
    calls.push(args);return {...active,payment_authoriser:true};
  }});
  assert.equal(result.ok,true);
  assert.deepEqual(calls,[[id,'id,is_active,role,payment_authoriser,payment_golden_key']]);
  const custom=await requireActivePaymentActor({},id,{readUser:async()=>active,
    onDenied:({status,code})=>new Response(JSON.stringify({ok:false,error_code:code}),{status})});
  assert.equal(custom.response.status,403);
  assert.deepEqual(await custom.response.json(),{ok:false,error_code:'PAYMENT_PERMISSION_REQUIRED'});
});

test('default helper uses a bounded single-user GET and never reads password data',async()=>{
  const oldFetch=globalThis.fetch; const calls=[];
  try{
    globalThis.fetch=async(url,options)=>{
      calls.push({url,options}); return Response.json([{...active,role:'admin'}]);
    };
    const result=await requireActivePaymentActor({SUPABASE_URL:'https://test.invalid',SUPABASE_SERVICE_ROLE_KEY:'synthetic-test-key'},id);
    assert.equal(result.ok,true); assert.equal(calls.length,1);
    const url=new URL(calls[0].url);
    assert.equal(url.pathname,'/rest/v1/tms_users');
    assert.equal(url.searchParams.get('id'),`eq.${id}`);
    assert.equal(url.searchParams.get('limit'),'1');
    assert.equal(url.searchParams.get('select'),'id,is_active,role,payment_authoriser,payment_golden_key');
    assert.ok(calls[0].options.signal instanceof AbortSignal);
    assert.doesNotMatch(calls[0].url,/password|email|session/);
  } finally {globalThis.fetch=oldFetch;}
});
