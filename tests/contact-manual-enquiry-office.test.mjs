import test from 'node:test';
import assert from 'node:assert/strict';
import {handleContactManualEnquiryOfficeCommand as handle,CONTACT_MANUAL_ENQUIRY_REFUSALS as refusals,
 CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS as commands,parseContactManualEnquiryBody as parse} from '../broker/src/weekly-source/contact-manual-enquiry-office.mjs';
import {fixture,id,copy} from './fixtures/contact-manual-enquiry-contract-fixture.mjs';

const user={id:id(80),role:'admin',is_active:true};
const deployment={agency_id:id(81),environment:'TEST'};
function harness(action='CONTACT_MANUAL_ENQUIRY_PREVIEW'){
 const f=fixture(),calls=[],body={action,payload:action.endsWith('PREVIEW')?f.request:f.accept};
 const options={body,user,deploymentContext:()=>deployment,rpc:async(name,request)=>{
  calls.push({name,request:copy(request)});return copy(action.endsWith('PREVIEW')?f.result:f.saved);
 }};
 return {f,calls,body,options};
}
async function result(options){const response=await handle(options);assert.equal(response.headers.get('cache-control'),'no-store');
 return {status:response.status,json:await response.json()};}
const failure=(code)=>({ok:false,error_code:code});

test('preview performs one exact native request with only authenticated actor and deployment',async()=>{
 const h=harness(),before=copy(h.body);const r=await result(h.options);
 assert.equal(r.status,200);assert.deepEqual(r.json,h.f.result);assert.deepEqual(h.body,before);
 assert.deepEqual(h.calls,[{name:commands.CONTACT_MANUAL_ENQUIRY_PREVIEW,request:{...h.f.request,actor_user_id:user.id,...deployment}}]);
 assert.equal(h.calls[0].request.scopes[0].expected_scope_revision,'9007199254740993');
});
test('accept acknowledges an immutable queued intent without a new preview or provider effect',async()=>{
 const h=harness('CONTACT_MANUAL_ENQUIRY_ACCEPT');const r=await result(h.options);
 assert.equal(r.status,200);assert.deepEqual(r.json,h.f.saved);assert.equal(r.json.provider_submission_started,false);
 assert.deepEqual(h.calls,[{name:commands.CONTACT_MANUAL_ENQUIRY_ACCEPT,request:{...h.f.accept,actor_user_id:user.id,...deployment}}]);
});
test('extra URL query is refused before deployment lookup or RPC',async()=>{
 const h=harness();h.options.search='?actor_user_id='+id(91);h.options.deploymentContext=()=>{throw Error('must not run');};
 const r=await result(h.options);assert.equal(r.status,400);assert.deepEqual(r.json,failure('CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID'));assert.equal(h.calls.length,0);
});
for(const change of [
 h=>{h.body.actor_user_id=id(90);},h=>{h.body.payload.actor_user_id=id(90);},
 h=>{h.body.payload.environment='LIVE';},h=>{h.body.payload.scopes[0].actor_user_id=id(90);},
 h=>{h.body.payload.scopes[0].expected_checking_revision=7;},h=>{h.body.action=' contact_manual_enquiry_preview ';},
])test('untrusted actor/deployment/coercion envelope is rejected without RPC: '+change.toString(),async()=>{
 const h=harness();change(h);const r=await result(h.options);assert.equal(r.status,400);assert.equal(h.calls.length,0);
});
for(const actor of [null,{...user,role:'user',payment_authoriser:true,payment_golden_key:true},{...user,is_active:false}])
 test('current active Office Admin is required: '+JSON.stringify(actor),async()=>{
  const h=harness();h.options.user=actor;const r=await result(h.options);
  assert.equal(r.status,403);assert.deepEqual(r.json,failure('WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED'));assert.equal(h.calls.length,0);
 });
test('unbound or failed deployment is unavailable and never invokes a native owner',async()=>{
 for(const context of [()=>null,()=>({...deployment,extra:'untrusted'}),()=>{throw Error('private diagnostic');}]){
  const h=harness();h.options.deploymentContext=context;const r=await result(h.options);
  assert.equal(r.status,503);assert.deepEqual(r.json,failure('CONTACT_DEPLOYMENT_UNAVAILABLE'));assert.equal(h.calls.length,0);
 }
});
for(const [code,status] of Object.entries(refusals))test('exact native public refusal maps '+code,async()=>{
 const h=harness();h.options.rpc=async(fn,request)=>{h.calls.push({fn,request});throw {fn,json:{message:code,details:'private native detail'}};};
 const r=await result(h.options);assert.equal(r.status,status);assert.deepEqual(r.json,failure(code));assert.equal(h.calls.length,1);
});
test('foreign RPC, untrusted code and arbitrary native faults return only bounded dependency failure',async()=>{
 for(const fault of [{fn:'another_rpc',json:{message:'CONTACT_SCOPE_NOT_FOUND'}},
  {code:'CONTACT_SCOPE_NOT_FOUND',message:'private SQL row'},
  {fn:commands.CONTACT_MANUAL_ENQUIRY_PREVIEW,json:{message:'private secret diagnostic'}},
  {fn:commands.CONTACT_MANUAL_ENQUIRY_PREVIEW,json:{message:'toString'}},
  {fn:commands.CONTACT_MANUAL_ENQUIRY_PREVIEW,json:{message:{toString:null}}},
  {fn:commands.CONTACT_MANUAL_ENQUIRY_PREVIEW,json:{message:null}},null]){
  const h=harness();h.options.rpc=async()=>{throw fault;};const r=await result(h.options);
  assert.equal(r.status,503);assert.deepEqual(r.json,failure('CONTACT_MANUAL_ENQUIRY_DEPENDENCY_UNAVAILABLE'));
 }
});
test('lost acceptance response is never retried implicitly and exact explicit retry preserves identity',async()=>{
 const h=harness('CONTACT_MANUAL_ENQUIRY_ACCEPT');const before=copy(h.body);
 h.options.rpc=async(name,request)=>{h.calls.push({name,request:copy(request)});if(h.calls.length===1)throw Error('connection lost');return copy(h.f.saved);};
 const first=await result(h.options);assert.equal(first.status,503);assert.equal(h.calls.length,1);assert.deepEqual(h.body,before);
 const second=await result(h.options);assert.equal(second.status,200);assert.deepEqual(h.calls[0],h.calls[1]);assert.deepEqual(second.json,h.f.saved);
});
test('malformed or foreign native reply never leaks its body as an acknowledgement',async()=>{
 const h=harness();h.f.result.groups[0].preview.review_url='private-provider-url';
 const r=await result(h.options);assert.equal(r.status,503);assert.deepEqual(r.json,failure('CONTACT_MANUAL_ENQUIRY_DEPENDENCY_UNAVAILABLE'));
});
test('receipt cannot substitute a different original request revision even without stored preview',async()=>{
 const h=harness('CONTACT_MANUAL_ENQUIRY_ACCEPT');h.f.saved.scope_results[0].checking_revision='8';
 const r=await result(h.options);assert.equal(r.status,503);assert.deepEqual(r.json,failure('CONTACT_MANUAL_ENQUIRY_DEPENDENCY_UNAVAILABLE'));
});
test('all-excluded review remains a successful read without any confirmation identity',async()=>{
 const h=harness();for(const member of h.f.result.scope_results[0].members){member.eligible=false;member.exclusion_reason='PAUSED';}
 Object.assign(h.f.result,{can_accept:false,groups:[],review_id:null,review_digest:null,expires_at_utc:null});
 const r=await result(h.options);assert.equal(r.status,200);assert.deepEqual(r.json,h.f.result);assert.equal(h.calls.length,1);
});
test('native parser rejects nested duplicate JSON keys before request construction',()=>{
 assert.throws(()=>parse('{"action":"CONTACT_MANUAL_ENQUIRY_PREVIEW","payload":{"reason":"first","re\\u0061son":"second"}}'),
  error=>error.code==='CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID');
 assert.throws(()=>parse(' '.repeat(262145)),error=>error.code==='CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID');
});
