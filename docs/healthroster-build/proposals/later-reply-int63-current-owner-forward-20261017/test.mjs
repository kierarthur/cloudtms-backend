// Actual complete Health Worker router, with only named external seams stubbed.
// No HTTP, DB, finance implementation or Office code executes.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
const read=f=>fs.readFileSync(new URL(f,import.meta.url),'utf8');
const source=read('./reference/routes.js');
const protocol=read('./reference/missing-accept.mjs').replace(/^export\s+/gm,'');
const bindings={normalizeBpayNextOwnerStatus:v=>v,
 isActivePaymentActor:u=>u.is_active===true&&(u.role==='admin'||u.payment_authoriser===true||u.payment_golden_key===true),
 isContactPauseCommand:()=>false,isContactManualEnquiryCommand:()=>false,isPaidMatchCommand:()=>false,
 matchSourcePayDecisionRoute:()=>null,bpayBusyEnvelope:()=>null};
Object.assign(bindings,await import('./reference/kept-pay-command.mjs'));
const executable=source.replace(/^import\s*\{([\s\S]*?)\}\s*from\s*['"]([^'"]+)['"];?/gm,(_m,names)=>{
 for(const spec of names.split(',').map(s=>s.trim()).filter(Boolean)){
  const[name,alias]=spec.split(/\s+as\s+/),id=alias??name;
  if(!(id in bindings))bindings[id]=()=>{throw Error('UNUSED_EXTERNAL_BOUNDARY:'+id);};
 }return '';
}).replace(/^export\s+(?=(?:async\s+)?(?:function|class|const|let|var)\b)/gm,'');
assert(!/^import\s|^export\s/m.test(executable));
const context=vm.createContext({...bindings,Request,Response,URL,TextEncoder,TextDecoder,Uint8Array,
 AbortController,setTimeout,clearTimeout,console,Buffer,__exports:{}});
vm.runInContext(protocol+'\n'+executable+'\n__exports.dispatch=dispatchWeeklySourceRequest;',context,{timeout:3000});
const actor='ffffffff-ffff-ffff-ffff-ffffffffffff',id='00000000-0000-0000-0000-000000000001';
const payload=(revision,decision='APPROVE_UPDATED_HOURS')=>({schema_version:'WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1',
 decision_bundle_id:id,bundle_revision:revision,root_timesheet_id:id,final_revision_id:id,decision,
 idempotency_key:'synthetic-later-reviewed-decision-1'});
async function route(p,result){
 let call;
 const response=await context.__exports.dispatch(new Request('https://source.invalid/api/weekly-source/v1/commands',{
  method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action:'LATER_CHANGE_DECISION',payload:p})}),{}, {},{
  requireUser:async()=>({id:actor,is_active:true,role:'office',payment_authoriser:true}),
  bpayNextOwnerStatusRpc:async()=>({active_owner:'NEXT'}),
  rpc:async(name,args)=>{call={name,args};return result;}});
 return {status:response.status,body:await response.json(),call};
}
for(const revision of ['1','9007199254740993','9223372036854775807'])test(`new retained text revision ${revision} round-trips through actual route`,async()=>{
 const result={ok:true,published:false,retained:true,decision:'KEEP_CURRENTLY_APPROVED_HOURS',
  decision_bundle_id:id,bundle_revision:revision,current_head_id:null,authority_kind:null,idempotent_replay:false};
 const r=await route(payload(revision,'KEEP_CURRENTLY_APPROVED_HOURS'),result);
 assert.equal(r.status,200);assert.deepEqual(r.body,result);
 assert.equal(r.call.args.p_request.bundle_revision,revision);
 assert.equal(r.call.args.p_request.actor_user_id,actor);
});
for(const revision of ['9007199254740993','9223372036854775806'])test(`new successor/reviewed text fields survive actual route ${revision}`,async()=>{
 const successor=(BigInt(revision)+1n).toString();
 const result={ok:true,published:true,retained:false,decision:'APPROVE_UPDATED_HOURS',
  decision_bundle_id:id,bundle_revision:successor,reviewed_bundle_revision:revision,
  authority_kind:'LOCKED_FINAL_SOURCE',idempotent_replay:false};
 const r=await route(payload(revision),result);assert.equal(r.status,200);assert.deepEqual(r.body,result);
 assert.equal(BigInt(r.body.bundle_revision),BigInt(r.body.reviewed_bundle_revision)+1n);
 assert.equal(r.call.args.p_request.bundle_revision,revision);
});
for(const stored of [{bundle_revision:3},{bundle_revision:4,reviewed_bundle_revision:3},
 {bundle_revision:'9007199254740994',reviewed_bundle_revision:'9007199254740993'}])test(`stored replay scalar types remain exact ${JSON.stringify(stored)}`,async()=>{
 const result={ok:true,...stored,idempotent_replay:true};
 const revision=typeof stored.bundle_revision==='number'?3:'9007199254740993';
 const r=await route(payload(revision),result);assert.equal(r.status,200);assert.deepEqual(r.body,result);
 assert.equal(typeof r.body.bundle_revision,typeof stored.bundle_revision);
 assert.equal(r.call.args.p_request.bundle_revision,revision);
});
for(const revision of ['0','01','+1','1.0','9223372036854775808',9007199254740992,null])test(`actual request boundary refuses ${String(revision)}`,async()=>{
 const r=await route(payload(revision),{ok:true});assert.equal(r.status,400);assert.equal(r.call,undefined);
});
test('exact historical numeric request remains numeric; new string request remains string',async()=>{
 const numeric=await route(payload(3),{ok:true,idempotent_replay:true,bundle_revision:4,reviewed_bundle_revision:3});
 const text=await route(payload('3'),{ok:true,idempotent_replay:false,bundle_revision:'4',reviewed_bundle_revision:'3'});
 assert.equal(numeric.status,200);assert.equal(text.status,200);
 assert.equal(numeric.call.args.p_request.bundle_revision,3);assert.equal(text.call.args.p_request.bundle_revision,'3');
});
test('unsafe historical JSON numbers cannot become lossless evidence by stringification',()=>{
 const old=JSON.parse('{"bundle_revision":9007199254740993}');
 assert.equal(Number.isSafeInteger(old.bundle_revision),false);
 assert.notEqual(String(old.bundle_revision),'9007199254740993');
 const fresh=JSON.parse('{"bundle_revision":"9007199254740993"}');
 assert.equal(BigInt(fresh.bundle_revision),9007199254740993n);
});
test('parsed actual seven outbound text expressions retain int63 extremities',()=>{
 const result=JSON.parse(read('./GRAMMAR_RESULTS.json'));assert.equal(result.outboundTextExpressions,7);
 for(const vector of result.wireVectors)for(const field of vector.parsedOutboundValues){
  assert.equal(typeof field.value,'string');assert.equal(BigInt(field.value),BigInt(vector.bigint));
 }
 assert.equal(result.unchangedStoredReplayReturns,2);
 assert.equal(result.nativeRequestAndBigintCASArithmeticUnchanged,true);
 assert.equal(result.native,'NOT_RUN');
});
