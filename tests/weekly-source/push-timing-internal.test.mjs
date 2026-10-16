import assert from 'node:assert/strict';
import test from 'node:test';
import { signWeeklySourceDeliveryRequest } from '../../broker/src/weekly-source/delivery-auth.mjs';
import { createPushTimingInternalHandler, decodePushTimingServiceReply, PUSH_TIMING_INTERNAL_PATH } from '../../broker/src/weekly-source/push-timing-internal.mjs';
import { weeklySourceDeploymentContext } from '../../broker/src/weekly-source/routes.js';
import { consumeWeeklyPushAdmission, readWeeklyPushTiming } from '../../candidate-broker/src/weekly-source-push-timing.js';
import { sendWeeklySourcePush } from '../../candidate-broker/src/weekly-source-push-providers.js';
import { weeklySourceDeliveryRuntimeInternals } from '../../broker/src/weekly-source/delivery-runtime.mjs';

const id = n => `f7160000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const hash = n => n.repeat(64);
const utc = offset => new Date(Date.now()+offset).toISOString().replace(/\.(\d{3})Z$/,'.$1000Z');
const env = { CANDIDATE_APP_ENVIRONMENT:'TEST',MYTMS_OFFICE_AGENCY_ID:id(19),
  WEEKLY_SOURCE_DELIVERY_SERVICE_SECRET:'nonsecret-local-fixture-hmac-key-long-enough' };
const read = () => ({ operation:'READ_TIMING',request:{dispatch_target_id:id(1),dispatch_command_id:id(2),target_snapshot_hash:hash('a'),provider:'APNS'} });
const consume = () => ({operation:'CONSUME_START',request:{permit_id:id(3),provider_attempt_id:id(4),
  dispatch_target_id:id(1),dispatch_command_id:id(2),target_snapshot_hash:hash('a'),provider:'APNS',content_hash:hash('b')}});
const consumed = () => ({ok:true,version:'CONTACT_CONSUMED_V1',permit_id:id(3),provider_attempt_id:id(4),call_once:true,consumed_at_utc:utc(0)});
const state = () => ({ok:true,version:'PUSH_TIMING_DISPATCH_STATE_V1',dispatch_target_id:id(1),policy_revision:'9007199254740993',
  policy_sha256:hash('b'),calendar_digest:hash('c'),allowed_now:false,reason:'QUIET',next_allowed_utc:utc(20000),expires_at_utc:utc(60000)});
async function signed(body, options={}) {
  const request=new Request('https://agency.internal'+PUSH_TIMING_INTERNAL_PATH+(options.search||''),{method:'POST',body:typeof body==='string'?body:JSON.stringify(body)});
  return signWeeklySourceDeliveryRequest(request,options.env||env);
}
function fixture(native = body => body.version ? state() : consumed()) {
  const calls=[];
  const handler=createPushTimingInternalHandler({deploymentContext:weeklySourceDeploymentContext,
    rpc:async(_env,name,args,options)=>{calls.push({name,request:args.p_request,options});return native(args.p_request);}});
  return {handler,calls};
}
test('actual signed READ is advisory with exact deployment injection and string revision',async()=>{
  const f=fixture();const response=await f.handler(await signed(read()),env);
  assert.equal(response.status,200);assert.equal((await response.json()).policy_revision,'9007199254740993');
  assert.deepEqual(f.calls[0],{name:'candidate_push_timing_for_dispatch_v1',request:{...read().request,
    version:'PUSH_TIMING_DISPATCH_READ_V1',agency_id:id(19),environment:'TEST'},options:{timeoutMs:10000}});
});
test('actual signed consume returns only the matched original native attempt capability',async()=>{
  const f=fixture();const response=await f.handler(await signed(consume()),env);
  assert.equal(response.status,200);assert.equal((await response.json()).call_once,true);
  assert.deepEqual(f.calls[0].request,{...consume().request,agency_id:id(19),environment:'TEST'});
});
for(const [label,mutate]of[
  ['agency',b=>({...b,request:{...b.request,agency_id:id(99)}})],
  ['actor',b=>({...b,request:{...b.request,actor_user_id:id(99)}})],
  ['environment',b=>({...b,request:{...b.request,environment:'LIVE'}})],
  ['number UUID',b=>({...b,request:{...b.request,dispatch_target_id:1}})],
  ['uppercase UUID',b=>({...b,request:{...b.request,dispatch_target_id:id(1).toUpperCase()}})],
  ['array hash',b=>({...b,request:{...b.request,target_snapshot_hash:[hash('a')]}})],
  ['wrong hash',b=>({...b,request:{...b.request,target_snapshot_hash:'a'}})],
  ['EMAIL',b=>({...b,request:{...b.request,provider:'POWER_AUTOMATE'}})],
  ['extra allow',b=>({...b,allowed_now:true})],
  ['unknown operation',b=>({...b,operation:'SEND'})]
])test(`service refuses ${label} before RPC`,async()=>{
  const f=fixture();const response=await f.handler(await signed(mutate(read())),env);
  assert.equal(response.status,400);assert.equal(f.calls.length,0);
});
test('duplicate and escaped duplicate fields cannot enter signed native request',async()=>{
  const f=fixture();const raw=JSON.stringify(read()).replace('"provider":"APNS"','"provider":"APNS","\\u0070rovider":"APNS"');
  assert.equal((await f.handler(await signed(raw),env)).status,400);assert.equal(f.calls.length,0);
});
test('anonymous, altered body, stale HMAC, wrong environment, method and query refuse without DB',async()=>{
  const f=fixture();const request=await signed(read());
  const stale=new Request(request);stale.headers.set('x-cloudtms-weekly-delivery-timestamp','1');
  const requests=[new Request(request.url,{method:'POST',body:'{}'}),new Request(request.url,{method:'POST',headers:request.headers,body:'{}'}),
    stale,await signed(read(),{env:{...env,CANDIDATE_APP_ENVIRONMENT:'LIVE'}}),new Request(request.url),await signed(read(),{search:'?agency_id=foreign'})];
  for(const value of requests)assert.equal((await f.handler(value,env)).status,401);
  assert.equal(f.calls.length,0);
});
test('missing trusted binding refuses without a speculative RPC',async()=>{
  const f=fixture();const response=await f.handler(await signed(read()),{...env,MYTMS_OFFICE_AGENCY_ID:''});
  assert.equal(response.status,503);assert.equal(f.calls.length,0);
});
for(const [label,change]of[
  ['numeric revision',v=>({...v,policy_revision:9007199254740992})],
  ['scope',v=>({...v,dispatch_target_id:id(90)})],
  ['contradictory allowed',v=>({...v,allowed_now:true})],
  ['extra permit',v=>({...v,permit_id:id(3)})]
])test(`advisory reply rejects ${label}`,()=>assert.throws(()=>decodePushTimingServiceReply(change(state()),read())));
test('consumer rejects foreign or malformed capability and exact replay remains no-call',()=>{
  for(const result of [{...consumed(),permit_id:id(90)},{...consumed(),provider_attempt_id:id(90)},
    {...consumed(),call_once:false},{...consumed(),extra:true}])assert.throws(()=>decodePushTimingServiceReply(result,consume()));
  const replay={ok:false,reason:'ALREADY_CONSUMED',provider_attempt_id:id(4),call_once:false};
  assert.deepEqual(decodePushTimingServiceReply(replay,consume()),replay);
});
test('broker uses only configured private binding with actual signed endpoint; no public fetch',async()=>{
  const f=fixture();const bindingEnv={...env,MYTMS_IDENTITY_DELIVERY:{fetch:req=>f.handler(req,env)}};
  assert.equal((await readWeeklyPushTiming(bindingEnv,read().request)).allowed_now,false);
  assert.equal((await consumeWeeklyPushAdmission(bindingEnv,consume().request)).call_once,true);
  assert.deepEqual(f.calls.map(x=>x.name),['candidate_push_timing_for_dispatch_v1','weekly_contact_submission_consume_v1']);
  await assert.rejects(()=>consumeWeeklyPushAdmission(env,consume().request));
});
async function apnsFixture(native) {
  const keys=await crypto.subtle.generateKey({name:'ECDSA',namedCurve:'P-256'},true,['sign','verify']);
  const bytes=Buffer.from(await crypto.subtle.exportKey('pkcs8',keys.privateKey));
  const pem=`-----BEGIN PRIVATE KEY-----\n${bytes.toString('base64')}\n-----END PRIVATE KEY-----`;
  const f=fixture(native);let calls=0;
  const source={env:{...env,MYTMS_IDENTITY_DELIVERY:{fetch:req=>f.handler(req,env)},APNS_KEY_ID:'TEST',APNS_TEAM_ID:'TEST',
    APNS_PRIVATE_KEY_P8:pem,APNS_TOPIC:'net.cloudtms.mytms.test',APNS_ENVIRONMENT:'sandbox'},
    target:{...read().request,content_hash:hash('b'),tranche_kind:'CANDIDATE_INITIAL',deep_link:{destination:'WEEKLY_SOURCE_REQUEST',request_id:id(5)}},
    token:'fake-local-device-token',attemptId:id(4),permitId:id(3),consumeBeforeUtc:utc(30000),
    fetchImpl:async()=>{calls++;return new Response('{}',{status:200});}};
  return {f,source,calls:()=>calls};
}
test('sole provider consumes immediately before first alert and replay cannot fetch again',async()=>{
  let used=false;const f=await apnsFixture(()=>{if(used)return{ok:false,reason:'ALREADY_CONSUMED',provider_attempt_id:id(4),call_once:false};used=true;return consumed();});
  assert.equal((await sendWeeklySourcePush(f.source)).outcome,'ACCEPTED');
  assert.equal((await sendWeeklySourcePush(f.source)).call_once,false);assert.equal(f.calls(),1);
});
test('consume uncertainty retains original AMBIGUOUS and does not call the fake provider',async()=>{
  const f=await apnsFixture(()=>{throw new Error('lost committed response');});
  assert.equal((await sendWeeklySourcePush(f.source)).outcome,'AMBIGUOUS');assert.equal(f.calls(),0);
});

test('exact native unconsumed expiry is no-network on the original attempt',async()=>{
  const f=await apnsFixture(()=>{throw new Error('CONTACT_PERMIT_EXPIRED');});
  const result=await sendWeeklySourcePush(f.source);
  assert.equal(result.outcome,'TRANSIENT_FAILURE');
  assert.deepEqual(result.bounded_error,{error_code:'CONTACT_PERMIT_EXPIRED_NO_SUBMISSION'});
  assert.deepEqual(result.bounded_provider_receipt,{proved_no_network:true});
  assert.equal(f.calls(),0);
});

for(const [label,status,value]of[
  ['extra field',409,{ok:false,error_code:'CONTACT_PERMIT_EXPIRED',call_once:true}],
  ['wrong HTTP status',503,{ok:false,error_code:'CONTACT_PERMIT_EXPIRED'}],
  ['generic conflict',409,{ok:false,error_code:'CONTACT_PERMIT_INVALID'}]
])test(`untrusted expiry-shaped ${label} retains uncertainty without fetching`,async()=>{
  const f=await apnsFixture(()=>consumed());
  f.source.env.MYTMS_IDENTITY_DELIVERY={fetch:async()=>new Response(JSON.stringify(value),{status})};
  assert.equal((await sendWeeklySourcePush(f.source)).outcome,'AMBIGUOUS');
  assert.equal(f.calls(),0);
});
test('late consume response is proved no-network here and cannot fetch after call window',async()=>{
  const f=await apnsFixture(()=>consumed());f.source.consumeBeforeUtc=utc(-1);
  const result=await sendWeeklySourcePush(f.source);
  assert.equal(result.outcome,'TRANSIENT_FAILURE');assert.equal(result.bounded_provider_receipt.proved_no_network,true);assert.equal(f.calls(),0);
});
test('direct provider or future unregistered purpose cannot bypass permit/central content owner',async()=>{
  const f=await apnsFixture(()=>consumed());
  await assert.rejects(()=>sendWeeklySourcePush({...f.source,permitId:undefined}));
  await assert.rejects(()=>sendWeeklySourcePush({...f.source,target:{...f.source.target,tranche_kind:'FUTURE_OTP'}}));
  assert.equal(f.f.calls.length,0);assert.equal(f.calls(),0);
});
for(const channel of ['PUSH','EMAIL'])test(`native ${channel} START replay cannot resubmit or overwrite original result`,async()=>{
  let sends=0,resultCalls=0;
  const result=await weeklySourceDeliveryRuntimeInternals.deliverTargets(env,{
    rpc:async(name,args)=>{
      if(name==='weekly_source_message_dispatch_target_claim_v1')return{claimed_count:1,targets:[{channel,dispatch_target_id:id(1),lease_token:id(6)}]};
      if(name==='weekly_source_message_dispatch_target_start_atomic_v1'){
        assert.equal(args.p_request.agency_id,id(19));assert.equal(args.p_request.environment,'TEST');
        return{ok:true,replay:true,provider_attempt_id:id(4),call_once:false,reason:'CHECK_ORIGINAL_ATTEMPT'};
      }
      if(name==='weekly_source_message_dispatch_target_result_atomic_v1')resultCalls++;
    },candidatePushFetch:async()=>{sends++;},sendManagerEmail:async()=>{sends++;}
  },'fixture-worker',1);
  assert.equal(sends,0);assert.equal(resultCalls,0);assert.equal(result.unstarted,1);
});
