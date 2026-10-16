import test from 'node:test';
import assert from 'node:assert/strict';
import {createTimesheetQueryContactDrain} from '../../broker/src/weekly-source/query-contact-drain.mjs';
const id='12222222-2222-4222-8222-222222222222',delivery='22222222-2222-4222-8222-222222222222';
const permit='32222222-2222-4222-8222-222222222222',other='42222222-2222-4222-8222-222222222222',agency='52222222-2222-4222-8222-222222222222';
const hash='a'.repeat(64),lease='native-email-lease';
const row=()=>({id,type:'TIMESHEET_QUERY',context_kind:'TIMESHEET_QUERY_DELIVERY',context_id:delivery,attempt_lease_token:lease});
const current=()=>({ok:true,version:'TIMESHEET_QUERY_RECOMPOSE_RESULT_V1',mail_outbox_id:id,query_delivery_id:delivery,original_delivery_id:delivery,
 disposition:'CURRENT',replacements:[{mail_outbox_id:id,query_delivery_id:delivery}],excluded:[],next_eligible_at_utc:null});
const admitted=()=>({ok:true,version:'CONTACT_START_RESULT_V1',owner_kind:'TIMESHEET_QUERY_OUTBOX',channel:'EMAIL',mail_outbox_id:id,query_delivery_id:delivery,
 contact_permit_id:permit,content_hash:hash,call_once:true});
function setup(overrides={}){
 const calls=[];
 const rpc=async(name,args)=>{
  calls.push({name,request:structuredClone(args.p_request)});
  if(overrides[name])return overrides[name](args.p_request);
  if(name==='timesheet_query_email_recompose_v1')return current();
  if(name==='weekly_contact_submission_start_v1')return admitted();
  if(name==='timesheet_query_email_contact_result_v1')return {ok:true,version:'TIMESHEET_QUERY_RESULT_RECORDED_V1',contact_permit_id:permit,mail_outbox_id:id,query_delivery_id:delivery,
   disposition:args.p_request.provider_result.status==='SUCCESS'?'ACCEPTED':'AMBIGUOUS'};
  if(name==='timesheet_query_email_recompose_ready_v1')return {ok:true,version:'TIMESHEET_QUERY_RECOMPOSE_READY_RESULT_V1',results:[]};
  throw Error(name);
 };
 return {calls,adapter:createTimesheetQueryContactDrain({rpc,deployment:{agency_id:agency,environment:'TEST'},workerId:'native-drain',leaseToken:lease})};
}
test('other outbox types make no contact RPC and remain allowed',async()=>{
 const s=setup();assert.deepEqual(await s.adapter.prepare({id,type:'INVOICE'}),{applicable:false,allowed:true});
 assert.deepEqual(await s.adapter.start({id,type:'REMITTANCE'}),{applicable:false,allowed:true});assert.equal(s.calls.length,0);
});
test('missing deployment holds queries while ordinary mail remains unchanged',async()=>{
 const adapter=createTimesheetQueryContactDrain({rpc:()=>{throw Error();},deployment:null,workerId:'native-drain',leaseToken:lease});
 assert.equal((await adapter.prepare(row())).allowed,false);assert.equal((await adapter.start({type:'OTHER'})).allowed,true);
});
test('recomposition must confirm exact native claimed row; replacement waits for native claim',async()=>{
 const s=setup();assert.equal((await s.adapter.prepare(row())).allowed,true);
 assert.deepEqual(Object.keys(s.calls[0].request).sort(),['version','mail_outbox_id','query_delivery_id','attempt_lease_token','worker_id','agency_id','environment'].sort());
 const changed=setup({timesheet_query_email_recompose_v1:()=>({...current(),disposition:'RECOMPOSED',replacements:[{mail_outbox_id:other,query_delivery_id:other}]})});
 assert.equal((await changed.adapter.prepare(row())).allowed,false);
 const corrupt=setup({timesheet_query_email_recompose_v1:()=>({...current(),replacements:[{mail_outbox_id:other,query_delivery_id:other}]})});
 assert.equal((await corrupt.adapter.prepare(row())).reason,'CONTACT_QUERY_NATIVE_RECOMPOSITION_UNAVAILABLE');
});
test('foreign native lease and altered response never reach START authority',async()=>{
 const s=setup();assert.equal((await s.adapter.prepare({...row(),attempt_lease_token:'foreign'})).allowed,false);assert.equal(s.calls.length,0);
 const corrupt=setup({weekly_contact_submission_start_v1:()=>({...admitted(),content_hash:'X'.repeat(64)})});
 assert.equal((await corrupt.adapter.start(row())).allowed,false);
});
test('final pause and existing admitted/unknown attempt both forbid provider admission',async()=>{
 for(const reason of ['NOT_STARTED_PAUSED','CHECK_ORIGINAL_ATTEMPT','OBSOLETE']){
  const s=setup({weekly_contact_submission_start_v1:()=>({ok:false,version:'CONTACT_START_RESULT_V1',owner_kind:'TIMESHEET_QUERY_OUTBOX',channel:'EMAIL',mail_outbox_id:id,
   query_delivery_id:delivery,reason,call_once:false,next_eligible_at_utc:reason==='NOT_STARTED_PAUSED'?'2026-10-19T00:00:00.000000Z':null})});
  assert.equal((await s.adapter.start(row())).allowed,false);
 }
});
test('one exact successful native permit admits once within the drain',async()=>{
 const s=setup();assert.equal((await s.adapter.start(row())).allowed,true);assert.equal((await s.adapter.start(row())).allowed,false);
 assert.equal(s.calls[0].request.owner_kind,'TIMESHEET_QUERY_OUTBOX');
});
test('START response loss holds without calling a provider or inventing a permit',async()=>{
 const s=setup({weekly_contact_submission_start_v1:()=>{throw Error('response lost');}});
 assert.equal((await s.adapter.start(row())).reason,'CONTACT_QUERY_START_OUTCOME_UNCONFIRMED');
 const r=await s.adapter.reconcileProviderBatch({ok:true,body:{results:[{outbox_id:id,status:'SUCCESS'}]}},[id],new Map([[id,row()]]));
 assert.equal(r[0].reason,'CONTACT_QUERY_ORIGINAL_PERMIT_MISSING');assert.equal(s.calls.filter(c=>c.name.includes('contact_result')).length,0);
});
test('actual one-item SUCCESS uses exact permit/token/hash native result and no generic patch',async()=>{
 const s=setup();await s.adapter.start(row());
 const out=await s.adapter.reconcileProviderBatch({ok:true,body:JSON.stringify({results:[{outbox_id:id,status:'SUCCESS',provider_message_id:'actual-response-id'}]})},[id,other],new Map([[id,row()],[other,{id:other,type:'INVOICE'}]]));
 assert.equal(out.length,1);assert.equal(out[0].disposition,'ACCEPTED');
 const request=s.calls.at(-1).request;assert.equal(request.contact_permit_id,permit);assert.equal(request.attempt_lease_token,lease);assert.equal(request.content_hash,hash);
 assert.deepEqual(request.provider_result,{status:'SUCCESS',provider_message_id:'actual-response-id',error:null});
});
test('transport failure, missing/duplicate/foreign results and provider FAILED retain ambiguity',async()=>{
 const responses=[{ok:false,body:'transport lost'},{ok:true,body:'{}'},{ok:true,body:{results:[{outbox_id:other,status:'SUCCESS'}]}},
  {ok:true,body:{results:[{outbox_id:id,status:'SUCCESS'},{outbox_id:id,status:'SUCCESS'}]}},{ok:true,body:{results:[{outbox_id:id,status:'FAILED',error:'response failure'}]}}];
 for(const response of responses){const s=setup();await s.adapter.start(row());const outcomes=await s.adapter.reconcileProviderBatch(response,[id],new Map([[id,row()]]));assert.equal(outcomes[0].disposition,'AMBIGUOUS');assert.notEqual(s.calls.at(-1).request.provider_result.status,'SUCCESS');}
});
test('lost native result receipt preserves original ownership and returns no false success',async()=>{
 const s=setup({timesheet_query_email_contact_result_v1:()=>{throw Error('SQL receipt lost');}});await s.adapter.start(row());
 const outcomes=await s.adapter.reconcileProviderBatch({ok:true,body:{results:[{outbox_id:id,status:'SUCCESS'}]}},[id],new Map([[id,row()]]));
 assert.equal(outcomes[0].disposition,'RESULT_UNCONFIRMED');assert.equal((await s.adapter.start(row())).allowed,false);
});
