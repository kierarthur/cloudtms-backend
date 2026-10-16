import {test} from 'node:test';
import assert from 'node:assert/strict';
import {runWeeklySourceContactRecheck,decodeContactRecheckResult} from '../broker/src/weekly-source/contact-recheck-runtime.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const item=(n,extra)=>({version:'CONTACT_RECHECK_TASK_RESULT_V1',task_id:id(n),...extra});
const options=()=>({deployment:{agency_id:id(10),environment:'TEST'},workerId:'actual-worker-1',limit:20});
const reply=()=>({ok:true,version:'CONTACT_RECHECK_BATCH_RESULT_V1',processed:3,held:1,selection_complete:false,results:[
 item(1,{disposition:'CONTINUE',phase:'SOURCE_WORKER',native_id:id(11),native_disposition:'CURRENT_CAPTURE_UNCHANGED'}),
 item(2,{disposition:'WAITING_ACTUAL_NATIVE_DUE',next_due_at_utc:'2026-10-17T07:00:00.000000Z'}),
 item(3,{disposition:'HELD_DEPENDENCY',code:'CONTACT_RECHECK_ITEM_HELD'}),
 item(4,{disposition:'COMPLETE',next_due_at_utc:null})]});
test('one actual RPC carries only existing server deployment/worker/batch context and preserves held-item progress',async()=>{
 const calls=[];const expected=reply();const result=await runWeeklySourceContactRecheck({rpc:async(...args)=>{calls.push(args);return {data:[expected]};}},options());
 assert.deepEqual(result,expected);assert.equal(calls.length,1);
 assert.deepEqual(calls[0],['weekly_source_contact_recheck_batch_v1',{p_request:{version:'CONTACT_RECHECK_BATCH_V1',agency_id:id(10),
  environment:'TEST',worker_id:'actual-worker-1',limit:20}},{timeoutMs:30000}]);
 assert.equal(JSON.stringify(calls).includes('now_utc'),false);
});
for(const mutation of [r=>r.selection_complete=true,r=>r.held=0,r=>r.processed=2,r=>r.results.push(r.results[0]),
 r=>r.results[0].native_disposition='SENT_BY_BROWSER',r=>r.results[0].phase='FAKE_QUEUE',r=>r.results[0].extra=true,
 r=>r.results[1].next_due_at_utc=null,r=>r.results[3].next_due_at_utc='2026-10-17T07:00:00.000000Z']){
 test('strict current reply rejects altered native ownership/completion operands '+mutation.toString(),()=>{
  const value=reply();mutation(value);assert.throws(()=>decodeContactRecheckResult(value,20),/CONTACT_RECHECK_REPLY_INVALID/);
 });
}
test('unknown RPC outcome remains unavailable and does not retry or enqueue a new owner',async()=>{
 let calls=0;await assert.rejects(runWeeklySourceContactRecheck({rpc:async()=>{calls++;return {error:{message:'private detail'}};}},options()),/CONTACT_RECHECK_UNAVAILABLE/);
 assert.equal(calls,1);
});
test('invalid service context and noninteger batch refuse before RPC',async()=>{
 for(const changed of [{limit:0},{limit:1.5},{deployment:{...options().deployment,now_utc:'browser'}},{workerId:''}]){
  let calls=0;await assert.rejects(runWeeklySourceContactRecheck({rpc:async()=>{calls++;}}, {...options(),...changed}),/CONTACT_RECHECK_REQUEST_INVALID/);assert.equal(calls,0);
 }
});

// Exercise the actual scheduler parent, including the new awaited contribution.
const {runWeeklySourceDelivery}=await import('../broker/src/weekly-source/delivery-runtime.mjs');
for(const outcome of ['current','unavailable','malformed'])test('actual scheduler preserves later delivery after contact '+outcome,async()=>{
 const calls=[];const scheduledAt='2026-10-16T10:00:00.000Z';
 const context={MYTMS_OFFICE_AGENCY_ID:id(10),CANDIDATE_APP_ENVIRONMENT:'TEST'};
 const result=await runWeeklySourceDelivery(context,{rpc:async(name,args)=>{
  calls.push(name);
  if(name==='weekly_source_query_scheduler_tick_v1'){assert.equal(args.p_request.now_utc,scheduledAt);return {ok:true};}
  if(name==='weekly_source_contact_recheck_batch_v1'){
   assert.deepEqual(args.p_request,{version:'CONTACT_RECHECK_BATCH_V1',agency_id:id(10),environment:'TEST',worker_id:'actual-worker-1',limit:20});
   if(outcome==='unavailable')throw Error('opaque private failure');
   return outcome==='malformed'?{...reply(),selection_complete:true}:reply();
  }
  if(name==='weekly_source_message_render_due_list_v1')return {due_count:0,rows:[]};
  if(name==='weekly_source_message_dispatch_claim_v1')return {claimed_count:0,commands:[]};
  if(name==='weekly_source_message_dispatch_target_claim_v1')return {claimed_count:0,targets:[]};
  throw Error('No external provider or unsupported continuation is allowed in this unit test');
 }},{limit:20,workerId:'actual-worker-1',scheduledStartedAtUtc:scheduledAt});
 assert.equal(result.ok,true);
 assert.deepEqual(calls.slice(0,5),['weekly_source_query_scheduler_tick_v1','weekly_source_contact_recheck_batch_v1',
  'weekly_source_message_render_due_list_v1','weekly_source_message_dispatch_claim_v1','weekly_source_message_dispatch_target_claim_v1']);
 assert.equal(calls.filter(name=>name==='weekly_source_contact_recheck_batch_v1').length,1);
 assert.equal(result.delivery.claimed,0);
 if(outcome==='current')assert.deepEqual(result.contact_recheck,reply());
 else assert.deepEqual(result.contact_recheck,{ok:false,error_code:'CONTACT_RECHECK_UNAVAILABLE',processed:0,held:0,results:[]});
});
