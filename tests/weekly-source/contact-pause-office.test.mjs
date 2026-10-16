import assert from 'node:assert/strict';
import test from 'node:test';
import {dispatchWeeklySourceRequest,WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT} from '../../broker/src/weekly-source/routes.js';

const id=n=>`f7160000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const env={CANDIDATE_APP_ENVIRONMENT:'TEST',MYTMS_OFFICE_AGENCY_ID:id(19)};
const inactive=()=>({paused:false,paused_until_utc:null,paused_until_local:null,reason:null});
const scope=()=>({scope_id:id(20),candidate_id:id(10),client_id:id(11),revision:'9007199254740993',
  worker:inactive(),manager:inactive(),can_pause:true,can_resume:true,in_flight:[]});
const body=action=>({action,payload:{candidate_id:id(10),client_id:id(11),...(action==='CONTACT_PAUSE_GET'?{}:
  {request_id:id(2),expected_revision:'9007199254740993',audience:'WORKER',note:'Owner checked'})}});
function native(b) {
  return b.action==='CONTACT_PAUSE_GET'?{ok:true,version:'CONTACT_PAUSE_STATE_V1',scope:scope()}
    :{ok:true,version:'CONTACT_PAUSE_RECEIPT_V1',request_id:b.payload.request_id,
      operation:b.action==='CONTACT_PAUSE'?'PAUSE':'RESUME',audience:b.payload.audience,changed:true,
      recheck_task_id:id(21),automatic_send:false,scope:scope()};
}
async function run(b,options={}) {
  const calls=[];
  const response=await dispatchWeeklySourceRequest(new Request('https://agency.invalid/api/weekly-source/v1/commands'+(options.search||''),
    {method:'POST',headers:{'content-type':'application/json'},body:options.raw??JSON.stringify(b)}),options.env||env,{},
    {requireUser:async()=>options.user??{id:id(1),role:'admin',is_active:true},
      rpc:async(...args)=>{calls.push(args);if(options.error)throw options.error;return options.reply??native(b);}});
  return{status:response.status,value:await response.json(),calls};
}
for(const action of ['CONTACT_PAUSE_GET','CONTACT_PAUSE','CONTACT_RESUME'])test(`actual ${action} command uses exact current native owner`,async()=>{
  const b=body(action),r=await run(b);
  assert.equal(r.status,200);assert.deepEqual(r.value,native(b));
  const rpc=WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.contactPauseRpcCommands[action];
  assert.deepEqual(r.calls,[[rpc,{p_request:{...b.payload,actor_user_id:id(1),agency_id:id(19),environment:'TEST'}},{timeoutMs:20000}]]);
  assert.equal(r.value.scope.revision,'9007199254740993');
});
for(const [label,mutate]of[
  ['actor',b=>({...b,payload:{...b.payload,actor_user_id:id(99)}})],
  ['agency',b=>({...b,payload:{...b.payload,agency_id:id(99)}})],
  ['environment',b=>({...b,payload:{...b.payload,environment:'LIVE'}})],
  ['extra root',b=>({...b,allow:true})],
  ['numeric revision',b=>({...b,payload:{...b.payload,expected_revision:9007199254740992}})],
  ['leading zero',b=>({...b,payload:{...b.payload,expected_revision:'01'}})],
  ['exponent',b=>({...b,payload:{...b.payload,expected_revision:'1e3'}})],
  ['overflow',b=>({...b,payload:{...b.payload,expected_revision:'9223372036854775808'}})],
  ['uppercase UUID',b=>({...b,payload:{...b.payload,candidate_id:id(10).toUpperCase()}})],
  ['array UUID',b=>({...b,payload:{...b.payload,candidate_id:[id(10)]}})],
  ['array action',b=>({...b,action:[b.action]})],
  ['note object',b=>({...b,payload:{...b.payload,note:{text:'x'}}})],
  ['long note',b=>({...b,payload:{...b.payload,note:'x'.repeat(501)}})],
  ['caller expiry',b=>({...b,payload:{...b.payload,paused_until_utc:'2026-10-19T00:00:00.000000Z'}})]
])test(`contact command refuses ${label} before RPC`,async()=>{
  const r=await run(mutate(body('CONTACT_PAUSE')));assert.equal(r.status,400);assert.equal(r.calls.length,0);
});
test('GET cannot create a scope or accept mutation fields',async()=>{
  const b=body('CONTACT_PAUSE_GET');b.payload.request_id=id(2);
  const r=await run(b);assert.equal(r.status,400);assert.equal(r.calls.length,0);
});
test('strict notes use PostgreSQL codepoints and preserve original retry bytes',async()=>{
  const b=body('CONTACT_PAUSE');b.payload.note='🙂'.repeat(500);
  const r=await run(b);assert.equal(r.status,200);assert.equal(r.calls[0][1].p_request.note,b.payload.note);
});
test('duplicate escaped command keys and malformed UTF8 refuse before RPC',async()=>{
  const b=body('CONTACT_PAUSE');
  const raw=JSON.stringify(b).replace('"audience":"WORKER"','"audience":"WORKER","\\u0061udience":"MANAGER"');
  const duplicate=await run(b,{raw});assert.equal(duplicate.status,400);assert.equal(duplicate.calls.length,0);
  const bad=await run(b,{raw:new Uint8Array([0xff])});assert.equal(bad.status,400);assert.equal(bad.calls.length,0);
});
test('query deployment injection, missing binding and payment-read projection cannot mutate pause',async()=>{
  for(const options of [{search:'?agency_id='+id(99)},{env:{...env,MYTMS_OFFICE_AGENCY_ID:''}},
    {user:{id:id(1),role:'payment_authoriser',is_active:true,can_view_payments:true}},
    {user:{id:id(1),role:'admin',is_active:false}}]){
    const r=await run(body('CONTACT_PAUSE'),options);assert.notEqual(r.status,200);assert.equal(r.calls.length,0);
  }
});
for(const [label,mutate]of[
  ['numeric revision',r=>({...r,scope:{...r.scope,revision:9007199254740992}})],
  ['foreign scope',r=>({...r,scope:{...r.scope,candidate_id:id(99)}})],
  ['extra policy',r=>({...r,policy:{enabled:false}})],
  ['automatic send',r=>({...r,automatic_send:true})],
  ['wrong acknowledgement',r=>({...r,request_id:id(99)})],
  ['no resume task',r=>({...r,recheck_task_id:null})]
])test(`malformed native ${label} never becomes an accepted acknowledgement`,async()=>{
  const b=body('CONTACT_RESUME'),r=await run(b,{reply:mutate(native(b))});
  assert.equal(r.status,503);assert.deepEqual(r.value,{ok:false,error_code:'CONTACT_DEPENDENCY_UNAVAILABLE'});
});
test('unchanged PAUSE may have no task; RESUME retains its actual bounded wake',async()=>{
  const b=body('CONTACT_PAUSE'),reply={...native(b),changed:false,recheck_task_id:null};
  assert.deepEqual((await run(b,{reply})).value,reply);
});
test('lost save and native conflicts do not automatically retry or claim unchanged state',async()=>{
  const b=body('CONTACT_PAUSE');
  for(const [code,status]of[['CONTACT_IDEMPOTENCY_CONFLICT',409],['CONTACT_REVISION_CONFLICT',409],['PGRST202',503]]){
    const r=await run(b,{error:{fn:'weekly_source_contact_pause_atomic_v1',json:{message:code}}});
    assert.equal(r.status,status);assert.equal(r.calls.length,1);assert.equal(r.value.ok,false);
  }
  const r=await run(b,{error:new Error('response lost')});assert.equal(r.status,503);assert.equal(r.calls.length,1);
});
test('exact older acknowledgement is retained without comparing it with current wall-clock',async()=>{
  const b=body('CONTACT_PAUSE'),reply=native(b);
  reply.scope.worker={paused:true,paused_until_utc:'2026-10-12T23:00:00.000000Z',
    paused_until_local:'2026-10-13 00:00 Europe/London',reason:'Prior genuine acknowledgement'};
  assert.deepEqual((await run(b,{reply})).value,reply);
});
test('GET carries an absent native scope without inventing a revision or write',async()=>{
  const b=body('CONTACT_PAUSE_GET'),reply=native(b);
  reply.scope.scope_id=null;reply.scope.revision='0';
  const r=await run(b,{reply});assert.equal(r.status,200);assert.deepEqual(r.value,reply);
  assert.equal(r.calls[0][0],'weekly_source_contact_pause_get_v1');
});
test('in-flight uncertainty is factual and cannot disclose another scope or full body',async()=>{
  const b=body('CONTACT_PAUSE_GET'),reply=native(b);
  reply.scope.in_flight=[{permit_id:id(30),owner_kind:'WEEKLY_SOURCE_TARGET',delivery_id:id(31),provider_attempt_id:id(32),
    channel:'PUSH',status:'AMBIGUOUS',started_at_utc:'2026-10-16T00:44:29.123456Z',
    memberships:[{scope_id:id(20),candidate_id:id(10),client_id:id(11),audience:'WORKER',revision:'9007199254740993'}]}];
  assert.deepEqual((await run(b,{reply})).value,reply);
  for(const change of [r=>{r.scope.in_flight[0].full_body='secret';},
    r=>{r.scope.in_flight[0].memberships[0].client_id=id(99);},r=>{r.scope.in_flight[0].memberships[0].revision=1;}]){
    const bad=structuredClone(reply);change(bad);const r=await run(b,{reply:bad});
    assert.equal(r.status,503);assert.deepEqual(r.value,{ok:false,error_code:'CONTACT_DEPENDENCY_UNAVAILABLE'});
  }
});
