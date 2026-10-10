import test from 'node:test';
import assert from 'node:assert/strict';
import {handleBpayNextBackgroundMessage,runBpayNextRecoveryTick} from '../broker/src/bpay-next-background.js';
import {classifyBpayNextContinuation,createBpayNextLegacyRetirement} from '../broker/src/bpay-next-legacy-retirement.js';
const JOB='b8f70000-0000-4000-8000-000000000001';
const RUN='b8f70000-0000-4000-8000-000000000002';
const NONCE='b8f70000-0000-4000-8000-000000000003';
const COMMAND='b8f70000-0000-4000-8000-000000000004';
const ENROLL={version:1,runtime_action:'ENROLL'};
const EXPIRY={version:1,runtime_action:'EXPIRY',run_id:RUN};
const enrollment=(extra={})=>({progressed:true,last_enrolled_sequence:'1',job_id:JOB,job_kind:'CASE_CREATE',job_status:'READY',...extra});
const receipt=()=>({command_id:COMMAND,run_id:RUN,sequence:'2',phase:'CANCELLING',expected_candidates:'2',completed_candidates:'0',replay:false});
const sql=(message,code='55000')=>Response.json({code,message,details:null,hint:null},{status:400});
function setup(body=ENROLL,overrides={}) {
  const events=[],calls=[],sent=[];
  const message={id:'native-shape-test',attempts:1,body,
    ack(){assert.equal(this,message);events.push('ack');},
    retry(options){assert.equal(this,message);events.push(['retry',options.delaySeconds]);}};
  const deps={
    async runtimeRpc(action,args){calls.push([action,args]);events.push(action);
      if(action==='ENROLL') return Response.json(enrollment());
      if(action==='WAKE_PAGE') return Response.json({lane:args.p_lane,items:[]});
      return Response.json({items:[],cursor_deadline:null,cursor_run_id:null});},
    async preparationExpiryRpc(action,args){calls.push([action,args]);events.push(action);return Response.json(receipt());},
    async financialJobRpc(action,id,kind,args){calls.push([action,id,kind,args]);events.push(action);
      return Response.json({job_id:id,job_kind:kind,phase:'NEW',lease_nonce:NONCE,
        owner_epoch:'3',lease_until_utc:'2026-10-04T17:00:00.000001Z',cursor:null});},
    queue:{async sendBatch(messages){events.push('send');sent.push(...structuredClone(messages));events.push('durable');}},
    async onFailure(){return 'RETRY';},...overrides};
  return {message,deps,events,calls,sent};
}
test('one enrollment persists CLAIM and next ENROLL before acknowledgement; no inline financial work',async()=>{
  const s=setup();assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'ACK');
  assert.deepEqual(s.events,['ENROLL','send','durable','ack']);assert.equal(s.calls.length,1);
  assert.deepEqual(s.sent,[{body:{version:1,action:'CLAIM',job_id:JOB,job_kind:'CASE_CREATE',lease_seconds:120},contentType:'json'},
    {body:ENROLL,contentType:'json'}]);
});
test('empty expiry enrollment progress wakes the next command without inventing a job',async()=>{
  const s=setup(ENROLL,{runtimeRpc:async()=>Response.json(enrollment({job_id:null,job_kind:null,job_status:null}))});
  assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'ACK');
  assert.deepEqual(s.sent,[{body:ENROLL,contentType:'json'}]);
});
test('no available command ends the enrollment chain; a completed job never gets another CLAIM',async()=>{
  for(const value of [enrollment({progressed:false,job_id:null,job_kind:null,job_status:null}),enrollment({job_status:'DONE'})]){
    const s=setup(ENROLL,{runtimeRpc:async()=>Response.json(value)});
    assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'ACK');
    assert.equal(s.sent.length,value.progressed?1:0);
    assert.equal(s.sent.some(item=>item.body.action==='CLAIM'),false);
  }
});
test('lost enrollment reply retries the original small message, never adopts a lease',async()=>{
  const s=setup(ENROLL,{runtimeRpc:async()=>{throw Error('secret');}});
  assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'RETRY');
  assert.deepEqual(s.message.body,ENROLL);assert.equal(s.sent.length,0);assert.equal(s.events.includes('ack'),false);
});
test('uncertain durable publication cannot acknowledge enrollment or expiry',async()=>{
  for(const body of [ENROLL,EXPIRY]){
    const s=setup(body,{queue:{async sendBatch(){throw Error('lost send receipt');}}});
    assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'RETRY');
    assert.equal(s.events.includes('ack'),false);assert.deepEqual(s.message.body,body);assert.equal(s.calls.length,1);
  }
});
test('expiry receives only exact run identity, persists enrollment then acknowledges',async()=>{
  const s=setup(EXPIRY);assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'ACK');
  assert.deepEqual(s.calls,[['RECEIVE',{p_run_id:RUN}]]);
  assert.deepEqual(s.events,['RECEIVE','send','durable','ack']);assert.deepEqual(s.sent,[{body:ENROLL,contentType:'json'}]);
});
test('a definite Office-won race is safe ACK but a damaged or unexpected state is not',async()=>{
  for(const error of ['BPAY_NEXT_EXPIRY_NOT_UNCONFIRMED','BPAY_NEXT_EXPIRY_DEADLINE_UNBOUND','unexpected55000']){
    const s=setup(EXPIRY,{preparationExpiryRpc:async()=>sql(error)});
    const result=await handleBpayNextBackgroundMessage(s.message,s.deps);
    assert.equal(result.disposition,error==='BPAY_NEXT_EXPIRY_NOT_UNCONFIRMED'?'ACK':'RETRY');assert.equal(s.sent.length,0);
  }
});
test('unknown expiry response never claims completion or sends a second intake',async()=>{
  let calls=0;const s=setup(EXPIRY,{preparationExpiryRpc:async()=>{calls++;return Response.json({secret:'wrong'});}});
  assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'RETRY');
  assert.equal(calls,1);assert.equal(s.sent.length,0);assert.deepEqual(s.message.body,EXPIRY);
});
test('financial CLAIM delegates once and durably retains the actual lease before ACK',async()=>{
  const observed=[];
  const s=setup({version:1,action:'CLAIM',job_id:JOB,job_kind:'CASE_CREATE',lease_seconds:120},
    {onStepMeasurement:(line,busy)=>{observed.push({line,busy});throw Error('logging failed');}});
  const result=await handleBpayNextBackgroundMessage(s.message,s.deps);
  assert.equal(result.reason,'CONTINUATION_PERSISTED');assert.deepEqual(s.events,['CLAIM','send','durable','ack']);
  assert.equal(s.calls.length,1);assert.deepEqual(s.sent[0].body,{version:1,action:'STEP',job_id:JOB,
    job_kind:'CASE_CREATE',lease_nonce:NONCE,owner_epoch:'3'});
  assert.equal(observed.length,1);assert.equal(observed[0].line.step,'CLAIM');
  assert.equal(observed[0].line.job_id,JOB);assert.equal(observed[0].line.rows,null);
  assert.equal(observed[0].busy,false);
  assert.equal(JSON.stringify(observed).includes(NONCE),false);
});
test('malformed runtime envelopes cannot smuggle financials, clocks or partial confirmation',async()=>{
  for(const body of [{...ENROLL,actor:'SYSTEM'},{...EXPIRY,observed_now:'yesterday'},
    {...EXPIRY,run_id:'bad'},{version:1,runtime_action:'CLAIM'},null]){
    const s=setup(body);assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'RETRY');
    assert.equal(s.calls.length,0);assert.equal(s.sent.length,0);assert.equal(s.events.includes('ack'),false);
  }
});
test('missing dependencies retry explicitly; missing Message methods throw instead of implicit ACK',async()=>{
  const s=setup(ENROLL,{queue:null});assert.equal((await handleBpayNextBackgroundMessage(s.message,s.deps)).disposition,'RETRY');
  await assert.rejects(handleBpayNextBackgroundMessage({body:ENROLL},s.deps),/INTERFACE_INVALID/);
});
test('one recovery tick visits four independent fixed lanes, sending only bounded IDs',async()=>{
  const s=setup(ENROLL,{runtimeRpc:async(action,args)=>{
    s.calls.push([action,args]);
    if(action==='ENROLL') return Response.json(enrollment({progressed:false,job_id:null,job_kind:null,job_status:null}));
    if(action==='WAKE_PAGE') return Response.json({lane:args.p_lane,items:[{job_id:JOB,job_kind:'CASE_CREATE',command_sequence:'1'}]});
    return Response.json({items:[{run_id:RUN,expected_deadline:'2026-10-01T10:00:00.000001Z'}],
      cursor_run_id:RUN,cursor_deadline:'2026-10-01T10:00:00.000001Z'});
  }});
  const result=await runBpayNextRecoveryTick(s.deps);assert.equal(result.ok,true);
  assert.deepEqual(s.calls,[['ENROLL',{}],['WAKE_PAGE',{p_lane:'READY',p_limit:100}],
    ['WAKE_PAGE',{p_lane:'LEASE_EXPIRED',p_limit:100}],['EXPIRY_PAGE',{p_limit:100}]]);
  assert.equal(s.sent.length,3);assert.deepEqual(s.sent[2].body,EXPIRY);assert.equal(s.events.includes('ack'),false);
});
test('one failing lane cannot starve the other recovery lanes and sends are all awaited',async()=>{
  const s=setup(ENROLL,{runtimeRpc:async(action,args)=>{
    s.calls.push([action,args]);if(action==='ENROLL') throw Error('secret');
    if(action==='WAKE_PAGE') return Response.json({lane:args.p_lane,items:[]});
    return Response.json({items:[],cursor_deadline:null,cursor_run_id:null});
  }});
  const result=await runBpayNextRecoveryTick(s.deps);assert.equal(result.ok,false);assert.equal(result.lanes.length,4);
  assert.equal(result.lanes.filter(item=>item.ok).length,3);assert.equal(s.calls.length,4);
  assert.equal(JSON.stringify(result).includes('secret'),false);
});
test('oversized expiry and job pages cannot produce unbounded queue messages',async()=>{
  const s=setup(ENROLL,{runtimeRpc:async(action,args)=>{
    if(action==='ENROLL') return Response.json(enrollment({progressed:false,job_id:null,job_kind:null,job_status:null}));
    if(action==='WAKE_PAGE') return Response.json({lane:args.p_lane,items:Array(101).fill({job_id:JOB,job_kind:'CASE_CREATE',command_sequence:'1'})});
    return Response.json({items:Array(101).fill({run_id:RUN,expected_deadline:'2026-10-01T10:00:00Z'}),cursor_run_id:RUN,cursor_deadline:'2026-10-01T10:00:00Z'});
  }});
  assert.equal((await runBpayNextRecoveryTick(s.deps)).ok,false);assert.equal(s.sent.length,0);
});

test('shared legacy classification admits exact original schemas without coercion or extra fields',()=>{
  const time='2026-10-10T12:00:00.000Z';
  const certificate={message_type:'WORKBENCH_SETTLED_CERTIFICATE_CONTINUATION_V8',contract_version:1,
    continuation_sequence:1,session_id:RUN,actor_user_id:COMMAND,certificate_uuid:JOB,lease_owner:'original-owner',enqueued_at_utc:time};
  const operation={version:1,operation_id:JOB,operation_type:'DRAFT_CREATE',pay_batch_id:null,root_operation_id:null,
    successor_relation:'SELF',phase:'INITIALISE',run_after_utc:null,source:'handleBankingPayCreateDraft',enqueued_at_utc:time};
  const policy=source=>source==='handleBankingPayCreateDraft'?{types:['DRAFT_CREATE'],relations:['SELF']}:null;
  const wake={message_type:'BANKING_PAY_WORKBENCH_DRAIN_WAKE_V1',contract_version:1,wake_depth:1,wake_id:JOB,
    wake_reason:'PRE_ENTRY_COMMITTED_WORK',origin:null,scope_kind:'GLOBAL',requested_profile:'NUDGE',
    previous_drain_invocation_id:null,session_id:null,candidate_id:null,actor_user_id:null,enqueued_at_utc:time};
  assert.equal(classifyBpayNextContinuation(certificate,policy),'LEGACY_CERTIFICATE_V8');
  assert.equal(classifyBpayNextContinuation(operation,policy),'LEGACY_OPERATION_V1');
  assert.equal(classifyBpayNextContinuation(wake,policy),'LEGACY_WORKBENCH_WAKE_V1');
  for(const body of [{...certificate,contract_version:'1'},{...certificate,actor_user_id:undefined},
    {...certificate,extra:true},{...certificate,enqueued_at_utc:'2026-10-10T24:00:00.000Z'},
    {...certificate,enqueued_at_utc:'2026-02-30T12:00:00.000Z'},
    {...operation,source:'UNKNOWN_SOURCE'},{...operation,successor_relation:'ROOT'},
    {...wake,scope_kind:'SESSION'},{...wake,wake_depth:33}])assert.equal(classifyBpayNextContinuation(body,policy),'UNKNOWN');
  assert.equal(classifyBpayNextContinuation(ENROLL,policy),'NEXT_CANDIDATE');
});

test('completed native certificate retirement awaits exact durable audit reread and replays one ID',async()=>{
  const certificate={message_type:'WORKBENCH_SETTLED_CERTIFICATE_CONTINUATION_V8',contract_version:1,
    continuation_sequence:1,session_id:RUN,actor_user_id:COMMAND,certificate_uuid:JOB,lease_owner:'original-owner',enqueued_at_utc:'2026-10-10T12:00:00.000Z'};
  const status={ok:true,workbench_session_id:RUN,session_version:1,progress_counter_version:1,authority_fence_generation:1,
    selected_constituent_count:1,session_current_ready:true,certificate_lifecycle:'SEALED_CURRENT',certificate_uuid:JOB,
    certificate_contract:'WORKBENCH_SETTLED_CERTIFICATION_V2',certification_id:'WORKBENCH_SETTLED_CERTIFICATION_V2:'+ 'a'.repeat(64),
    overall_digest_sha256:'a'.repeat(64),certificate_ready_for_draft:true,certificate_build_pending:false,
    certificate_continue_polling:false,certificate_recovery_required:false,certificate_error_code:null,certificate_error_message:null,
    maximum_selected_constituents:50000,no_candidate_locks:true,read_only:true};
  const nativeId='b8f70000000040008000000000000005',events=[];let stored=null,postCount=0,loseReply=true;
  const token='x.'+Buffer.from(JSON.stringify({role:'service_role'})).toString('base64url')+'.y';
  const retire=createBpayNextLegacyRetirement({baseUrl:'http://127.0.0.1:55440',headers:{authorization:'Bearer '+token},
    fetchImpl:async(url,init)=>{
      assert.equal(init.redirect,'error');assert.ok(init.signal instanceof AbortSignal);
      if(url.includes('/rpc/pay_workbench_settled_certificate_status_v8')){
        assert.deepEqual(JSON.parse(init.body),{p_workbench_session_id:RUN,p_actor_user_id:COMMAND});events.push('qualify');return Response.json(status);
      }
      if(init.method==='POST'){
        assert.ok(url.endsWith('/audit_events'));events.push('insert');postCount++;
        const full=JSON.parse(init.body);stored=Object.fromEntries(['id','object_type','object_id_text','action','reason','correlation_id','after_json'].map(k=>[k,full[k]]));
        assert.equal(JSON.stringify(stored).includes('original-owner'),false);
        if(loseReply){loseReply=false;throw Error('uncertain insert');}return new Response(null,{status:201});
      }
      events.push('reread');return Response.json(stored?[stored]:[]);
    }});
  const message={id:nativeId,body:certificate};
  await assert.rejects(retire(message,'LEGACY_CERTIFICATE_V8','3'));
  assert.deepEqual(events,['qualify','reread','insert']);
  const outcome=await retire(message,'LEGACY_CERTIFICATE_V8','3');
  assert.equal(outcome.retired,true);assert.equal(outcome.created,false);assert.equal(postCount,1);
  assert.deepEqual(events.slice(3),['qualify','reread']);
  stored.after_json.certificate_uuid=NONCE;
  assert.equal((await retire(message,'LEGACY_CERTIFICATE_V8','3')).retired,false);assert.equal(postCount,1);
  stored.after_json.certificate_uuid=JOB;
  status.certificate_build_pending=true;
  const beforePending=events.length;
  assert.equal((await retire(message,'LEGACY_CERTIFICATE_V8','3')).retired,false);assert.equal(postCount,1);
  assert.deepEqual(events.slice(beforePending),['qualify']);
});

test('unknown native scope and unsupported message IDs cannot manufacture retirement receipts',async()=>{
  let calls=0;
  const token='x.'+Buffer.from(JSON.stringify({role:'service_role'})).toString('base64url')+'.y';
  const retire=createBpayNextLegacyRetirement({baseUrl:'http://127.0.0.1:55440',headers:{authorization:'Bearer '+token},
    fetchImpl:async()=>{calls++;throw Error('must not read/write');}});
  for(const message of [{id:'unsupported-format',body:{}},{id:'b8f70000000040008000000000000005',body:{scope_kind:'GLOBAL'}}]){
    assert.equal((await retire(message,'LEGACY_WORKBENCH_WAKE_V1','3')).retired,false);
  }
  assert.equal(calls,0);
});
