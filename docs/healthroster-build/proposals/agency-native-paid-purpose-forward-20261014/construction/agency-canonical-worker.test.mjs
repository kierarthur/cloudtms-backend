import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {decodeAgencyWorkerTargets,decodeAgencyWorkerContext,retainAgencySourceEventProvenance,
  runAgencyCanonicalManual,runAgencyLeasedCanonicalRecompute} from './runtime/agency-canonical-worker.mjs';
// Effect-free UNIT_ONLY records, never native evidence or qualification inputs.
// Financial calculation itself is deliberately outside this transport test.
const here=path.dirname(fileURLToPath(import.meta.url));
const u=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const copy=x=>JSON.parse(JSON.stringify(x));
const req=u(1),ts=u(2),fin=u(3),cw=u(4),contract=u(5),candidate=u(6),client=u(7),actor=u(8),cycle=u(9),event=u(10),outbox=u(11),other=u(12),newFin=u(13);
const entry={date:'2026-10-09',start:'09:00',end:'17:00',end_date:'2026-10-09',break_minutes:30,source_shift_id:event,work_event_id:event};
function frame(phase='RECOMPUTE_PENDING'){
 return {schema_version:'WEEKLY_SOURCE_AGENCY_CANONICAL_WORKER_CONTEXT_V1',request_id:req,request_sha256:'a'.repeat(64),
  request_version:7,phase,phase_actor_user_id:actor,current_row_signature:'UNIT_ONLY_NATIVE_SIGNATURE',
  source_cas:{schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:cycle,client_ids:[client],source_cas_sha256:'b'.repeat(64)},
  source_schedule:[copy(entry)],outbox_id:phase==='UNAUTHORISED'?null:outbox,outbox_reason:phase==='UNAUTHORISED'?null:'MANUAL',
  outbox_row:phase==='UNAUTHORISED'?null:{id:outbox,timesheet_id:ts,reason:'MANUAL',attempt_count:1,
    next_attempt_at:'2026-10-14T12:05:00Z',last_error:null,created_at:'2026-10-14T12:00:00Z'},
  context:{effective_timesheet_id:ts,out_timesheet:{timesheet_id:ts,contract_id:contract,version:1,sheet_scope:'WEEKLY',line_type:'HOURS',
    is_current:true,authorised_at_server:null,actual_schedule_json:[copy(entry)]},out_cur_fin:{id:fin,timesheet_id:ts,
    timesheet_version:1,locked_by_invoice_id:null,paid_at_utc:null},out_candidate:{id:candidate},out_umbrella:null,
    out_client_id:client,out_effective_flags:{},out_policy:{weekly_rate_classification_method:'SPLIT_RATE_WINDOWS'}},
  weekly_context:{timesheet_id:ts,out_cw:{id:cw,timesheet_id:ts,contract_id:contract},out_contract:{id:contract,candidate_id:candidate,client_id:client}},
  weekly_validation:null,nhsp_shifts:[],context_sha256:'c'.repeat(64)};
}
function snapshot(){return {timesheet_id:ts,timesheet_version:1,candidate_id:candidate,client_id:client,
 total_pay_ex_vat:123,total_charge_ex_vat:234,policy_snapshot_json:{unit_only:true},
 invoice_breakdown_json:{mode:'SEGMENTS',segments:[{segment_id:'UNIT_ONLY_SEGMENT',date:entry.date,start:entry.start,end:entry.end,
  break_mins:30,is_reversal:false,pay_amount:123,charge_amount:234}]}};}
function receipt(f,patch={}){return {schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_WORKER_RESULT_V1',request_id:req,
 request_version:f.request_version+4,phase:f.phase==='UNAUTHORISED'?'RECOMPUTE_PENDING':'FINANCIAL_CURRENT',
 phase_actor_user_id:actor,timesheet_id:ts,timesheet_version:1,current_financial_id:newFin,outbox_id:outbox,
 last_refusal_code:null,native_write_observed:true,native_queue_completed:f.phase!=='UNAUTHORISED',...patch};}
function options(f=frame(),patch={}){
 const calls=[];return {calls,requestId:req,leasedRows:[{outbox_id:outbox,timesheet_id:ts,reason:'MANUAL'}],
  readContext:async id=>{calls.push(['READ',id]);return copy(f);},
  buildWeeklySnapshot:async (...args)=>{calls.push(['BUILD',...args]);return {ok:true,snapshot:snapshot()};},
  applySnapshot:async args=>{calls.push(['APPLY',args]);return receipt(f);},...patch};}

test('targets are empty only from genuine closed native [] and preserve requested IDs',()=>{
 assert.equal(decodeAgencyWorkerTargets([],[ts]).size,0);
 assert.equal(decodeAgencyWorkerTargets([{request_id:req,timesheet_id:ts,phase:'RECOMPUTE_PENDING',outbox_id:outbox}],[ts]).get(ts).request_id,req);
});
for(const [name,value] of [
 ['foreign target',[{request_id:req,timesheet_id:other,phase:'RECOMPUTE_PENDING',outbox_id:outbox}]],
 ['duplicate target',[{request_id:req,timesheet_id:ts,phase:'RECOMPUTE_PENDING',outbox_id:outbox},{request_id:req,timesheet_id:ts,phase:'RECOMPUTE_PENDING',outbox_id:outbox}]],
 ['asserted capability',[{request_id:req,timesheet_id:ts,phase:'RECOMPUTE_PENDING',outbox_id:outbox,qualified:true}]],
 ['completed ACTIVE target',[{request_id:req,timesheet_id:ts,phase:'COMPLETED',outbox_id:outbox}]],
 ['missing queued identity',[{request_id:req,timesheet_id:ts,phase:'RECOMPUTE_PENDING',outbox_id:null}]]])
 test(`target refusal: ${name}`,()=>assert.throws(()=>decodeAgencyWorkerTargets(value,[ts]),/AGENCY_CHANGE_WORKER_TARGETS_INVALID/));

for(const [name,mutate] of [
 ['foreign request',f=>f.request_id=other],['wrong native phase',f=>f.phase='AUTHORISED'],
 ['unknown field',f=>f.native_qualified=true],['missing real actor',f=>f.phase_actor_user_id=null],
 ['current family rotation',f=>f.context.out_timesheet.timesheet_id=other],
 ['foreign contract',f=>f.weekly_context.out_contract.id=other],
 ['foreign current Candidate',f=>f.context.out_candidate.id=other],
 ['foreign native Client',f=>f.context.out_client_id=other],
 ['unknown policy',f=>f.context.out_policy=null],['source client rotation',f=>f.source_cas.client_ids=[other]],
 ['ambiguous event',f=>f.source_schedule.push(copy(entry))],['lease from other Timesheet',f=>f.outbox_row.timesheet_id=other]])
 test(`context refusal before calculation: ${name}`,async()=>{
  const f=frame();mutate(f);const o=options(f);
  await assert.rejects(runAgencyLeasedCanonicalRecompute(o),/AGENCY_CHANGE_WORKER_CONTEXT_INVALID/);
  assert.deepEqual(o.calls.map(x=>x[0]),['READ']);});

test('provenance retains complete canonical economics and actual event identity without recalculating',()=>{
 const input=snapshot();const output=retainAgencySourceEventProvenance(input,[entry]);
 assert.equal(output.total_pay_ex_vat,input.total_pay_ex_vat);assert.equal(output.total_charge_ex_vat,input.total_charge_ex_vat);
 assert.deepEqual(output.policy_snapshot_json,input.policy_snapshot_json);
 assert.deepEqual(output.invoice_breakdown_json.segments[0],{...input.invoice_breakdown_json.segments[0],work_event_id:event,source_shift_id:event});
 assert.deepEqual(output.actual_schedule_json,[entry]);assert.equal(input.actual_schedule_json,undefined);
});
test('two genuine same-day event tuples stay distinct; an identical interval is ambiguous',()=>{
 const e2={...entry,start:'18:00',end:'20:00',break_minutes:0,work_event_id:other,source_shift_id:other};
 const s=snapshot();s.invoice_breakdown_json.segments.push({...s.invoice_breakdown_json.segments[0],segment_id:'SECOND',start:'18:00',end:'20:00',break_mins:0});
 assert.deepEqual(retainAgencySourceEventProvenance(s,[entry,e2]).invoice_breakdown_json.segments.map(x=>x.work_event_id),[event,other]);
 assert.throws(()=>retainAgencySourceEventProvenance(s,[entry,{...entry,work_event_id:other,source_shift_id:other}]),/PROVENANCE_INVALID/);
});
for(const [name,mutate] of [
 ['foreign event annotation',s=>s.invoice_breakdown_json.segments[0].work_event_id=other],
 ['invoice-locked component',s=>s.invoice_breakdown_json.segments[0].invoice_locked_invoice_id=other],
 ['reversal component',s=>s.invoice_breakdown_json.segments[0].is_reversal=true],
 ['wrong actual interval',s=>s.invoice_breakdown_json.segments[0].start='08:00'],
 ['subset',s=>s.invoice_breakdown_json.segments=[]]])
 test(`provenance refusal: ${name}`,()=>{const s=snapshot();mutate(s);assert.throws(()=>retainAgencySourceEventProvenance(s,[entry]),/PROVENANCE_INVALID/);});
test('actual MANUAL and queued seams use same calculator once, closed native context stamp and no write_now',async()=>{
 for(const phase of ['UNAUTHORISED','RECOMPUTE_PENDING']){const f=frame(phase),o=options(f);
  const result=await(phase==='UNAUTHORISED'?runAgencyCanonicalManual(o):runAgencyLeasedCanonicalRecompute(o));
  assert.equal(result.phase,phase==='UNAUTHORISED'?'RECOMPUTE_PENDING':'FINANCIAL_CURRENT');
  assert.deepEqual(o.calls.map(x=>x[0]),['READ','BUILD','APPLY']);
  assert.equal(o.calls[1][5].write_now,false);assert.deepEqual(o.calls[1][1].actual_schedule_json,[entry]);
  assert.equal(o.calls[2][1].p_expected_request_version,f.request_version);
  assert.equal(o.calls[2][1].p_expected_context_sha256,f.context_sha256);
  assert.deepEqual(Object.keys(o.calls[2][1]).sort(),['p_expected_context_sha256','p_expected_request_version','p_request_id','p_snapshot']);
 }});
for(const [name,lease] of [['missing owned lease',[]],['foreign lease',[{outbox_id:other,timesheet_id:ts,reason:'MANUAL'}]],
 ['wrong lease reason',[{outbox_id:outbox,timesheet_id:ts,reason:'REVOKED'}]],
 ['wrong lease physical target',[{outbox_id:outbox,timesheet_id:other,reason:'MANUAL'}]]])
 test(`queue refusal before calculation: ${name}`,async()=>{const o=options(frame(),{leasedRows:lease});
  await assert.rejects(runAgencyLeasedCanonicalRecompute(o),/AGENCY_CHANGE_NATIVE_OUTBOX_NOT_LEASED/);
  assert.deepEqual(o.calls.map(x=>x[0]),['READ']);});
test('unavailable calculator never calls native apply',async()=>{const o=options(frame(),{buildWeeklySnapshot:async()=>({ok:false})});
 await assert.rejects(runAgencyLeasedCanonicalRecompute(o),/AGENCY_CHANGE_CANONICAL_BUILD_UNAVAILABLE/);assert.equal(o.calls.some(x=>x[0]==='APPLY'),false);});
test('native count/status/absence alone cannot assert financial current or queue completion',async()=>{
 for(const result of [{ok_count:1,fail_count:0,errors:[]},{},{...receipt(frame()),native_write_observed:false},
  {...receipt(frame()),current_financial_id:fin},{...receipt(frame()),outbox_id:other}]){
  const o=options(frame(),{applySnapshot:async()=>result});await assert.rejects(runAgencyLeasedCanonicalRecompute(o),/RESULT_UNCONFIRMED/);}
});
test('causal native hold remains actual old phase and never emits completion',async()=>{
 const o=options(frame(),{applySnapshot:async()=>receipt(frame(),{phase:'RECOMPUTE_PENDING',last_refusal_code:'AGENCY_CHANGE_ORIGINAL_SOURCE_CAS_CHANGED',native_write_observed:false,native_queue_completed:false,current_financial_id:fin})});
 await assert.rejects(runAgencyLeasedCanonicalRecompute(o),/AGENCY_CHANGE_ORIGINAL_SOURCE_CAS_CHANGED/);
});

const reverse=JSON.parse(fs.readFileSync(path.join(here,'WORKER_RUNTIME_FULL_REVERSE.json')));
function workerSource(file,expected){const source=fs.readFileSync(path.join(here,file),'utf8');
 assert.equal(crypto.createHash('sha256').update(source).digest('hex'),expected);
 const start=source.indexOf('async function runTsfinWorkerOnce(');
 const end=source.indexOf('async function handleTimesheetAuthoriseGeneric(',start);
 assert.ok(start>=0&&end>start);return source.slice(start,end).trim();}
const actualBefore=workerSource('runtime-reference/index.js',reverse.before_raw_sha256);
const actualAfter=workerSource('runtime-forward/index.js',reverse.after_raw_sha256);
function workerHarness({targets=[],targetError=null,lease=[{outbox_id:outbox,timesheet_id:ts,reason:'MANUAL'}],extraFail=false,nativeResult=receipt(frame()),source=actualAfter}={}){
 const calls=[],f=frame();f.context.out_cur_fin.paid_at_utc='2026-10-09T00:00:00Z';
 const deps={decodeAgencyWorkerTargets,runAgencyLeasedCanonicalRecompute,
  rpcTsfinDequeueBatchIds:async()=>copy(lease),rpcTsfinResolveRatesBatch:async()=>[],
  rpcTsfinLoadContextBatch:async()=>[copy(f.context)],
  sbFetch:async()=>({rows:[{timesheet_id:ts,booking_id:'UNIT_ONLY_BOOKING',is_current:true,version:1,updated_at:null,created_at:null}]}),
  sbRpc:async(_env,name,args)=>{calls.push([name,copy(args)]);
   if(name==='weekly_source_agency_change_worker_targets_v1'){if(targetError)throw targetError;return copy(targets);}
   if(name==='weekly_source_agency_change_worker_context_v1')return copy(f);
   if(name==='tsfin_work_fail'){if(extraFail&&args.p_id===other)throw new Error('UNIT_ONLY_QUEUE_DELIVERY_FAILURE');return null;}
   if(name==='tsfin_work_success')return null;
   throw new Error(`UNIT_ONLY_UNEXPECTED_RPC:${name}`);},
  sbRpcRecordingGuardRefusal:async(_env,name,args)=>{calls.push([name,copy(args)]);
   assert.equal(name,'weekly_source_agency_change_worker_apply_v1');return copy(nativeResult);},
  buildWeeklyScheduleSegmentsSnapshot:async()=>({ok:true,snapshot:snapshot()})};
 const names=Object.keys(deps);const create=new Function('deps',`const {${names.join(',')}}=deps;\n${source}\nreturn runTsfinWorkerOnce;`);
 return {calls,run:()=>create(deps)({SUPABASE_URL:'UNIT_ONLY_NO_NETWORK'})};
}
test('whole actual Worker: ordinary paid queue behavior is identical before/after native empty target census',async()=>{
 const before=workerHarness({source:actualBefore}),after=workerHarness();
 const a=await before.run(),b=await after.run();assert.deepEqual(a,b);
 assert.deepEqual(before.calls.filter(x=>x[0]==='tsfin_work_success'),after.calls.filter(x=>x[0]==='tsfin_work_success'));
 assert.equal(after.calls.some(x=>x[0]==='weekly_source_agency_change_worker_apply_v1'),false);
});
test('whole actual Worker: unknown native target state cannot fall through to ordinary paid success',async()=>{
 const h=workerHarness({targetError:new Error('UNIT_ONLY_NATIVE_DEPENDENCY_UNAVAILABLE')});const result=await h.run();
 assert.equal(result.ok,0);assert.equal(result.fail,1);assert.deepEqual(result.completed_timesheet_ids,[]);
 assert.equal(h.calls.some(x=>x[0].startsWith('tsfin_work_success')),false);
});
test('whole actual Worker: exact nonprimary MANUAL lease completes once, unrelated REVOKED remains',async()=>{
 const h=workerHarness({targets:[{request_id:req,timesheet_id:ts,phase:'RECOMPUTE_PENDING',outbox_id:outbox}],
  lease:[{outbox_id:other,timesheet_id:ts,reason:'REVOKED'},{outbox_id:outbox,timesheet_id:ts,reason:'MANUAL'}]});
 const result=await h.run();assert.equal(result.ok,1);assert.equal(result.fail,1);assert.deepEqual(result.completed_timesheet_ids,[ts]);
 assert.equal(h.calls.filter(x=>x[0]==='weekly_source_agency_change_worker_apply_v1').length,1);
 assert.deepEqual(h.calls.filter(x=>x[0]==='tsfin_work_fail').map(x=>x[1].p_id),[other]);
 assert.equal(h.calls.some(x=>x[0].startsWith('tsfin_work_success')),false);
});
test('whole actual Worker: later unrelated failure never labels the observed owned outbox failed',async()=>{
 const h=workerHarness({extraFail:true,targets:[{request_id:req,timesheet_id:ts,phase:'RECOMPUTE_PENDING',outbox_id:outbox}],
  lease:[{outbox_id:other,timesheet_id:ts,reason:'REVOKED'},{outbox_id:outbox,timesheet_id:ts,reason:'MANUAL'}]});
 await assert.rejects(h.run(),/UNIT_ONLY_QUEUE_DELIVERY_FAILURE/);
 assert.equal(h.calls.filter(x=>x[0]==='weekly_source_agency_change_worker_apply_v1').length,1);
 assert.equal(h.calls.some(x=>x[0]==='tsfin_work_fail'&&x[1].p_id===outbox),false);
});
