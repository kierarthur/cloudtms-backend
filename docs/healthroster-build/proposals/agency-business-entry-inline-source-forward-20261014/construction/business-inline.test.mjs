import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';
import assert from 'node:assert/strict';import test from 'node:test';import {fileURLToPath} from 'node:url';
import {agencyBusinessRequest,decodeAgencyBusinessResult,decodeAgencyInlineReply,createAgencyTimesheetChangeRuntime} from './agency-timesheet-change-runtime.mjs';
import {isActivePaymentActor} from './payment-actor-permission.js';
import {runAgencyCanonicalManual} from './83_complete_canonical_worker_with_manual_typed_validation.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),read=p=>fs.readFileSync(path.join(here,p),'utf8');
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
const u=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const I={actor:u(1),week:u(2),request:u(3),ts:u(4),other:u(5)};
const payload={contract_week_id:I.week,reason:'Align the finalised source',expected_signature:'native-click-signature',request_key:'genuine-click-key'};
const args=agencyBusinessRequest(payload,I.actor);
function reply(phase='UNAUTHORISED',version=4,extra={}){
 const completed=phase==='COMPLETED';return {schema_version:'WEEKLY_SOURCE_AGENCY_BUSINESS_RESULT_V1',
 request_id:I.request,request_key:payload.request_key,request_sha256:'a'.repeat(64),request_version:version,
 state:completed?'COMPLETED':'ACTIVE',phase,view_actor_user_id:I.actor,contract_week_id:I.week,
 timesheet_id:I.ts,timesheet_version:3,current_row_signature:'native-current-signature',completed,
 completed_actor_user_id:completed?I.actor:null,return_invoice_no:completed?'D1-actual':null,
 completion_sha256:completed?'b'.repeat(64):null,last_refusal_code:null,return_hold:!completed,
 continuation:completed?'NONE':phase==='UNAUTHORISED'?'MANUAL':['RECOMPUTE_PENDING','AUTHORISED'].includes(phase)?'WORKER':'NONE',
 code:completed?'OK':'AGENCY_INVOICE_TSFIN_NOT_READY',...extra};}
const copy=v=>JSON.parse(JSON.stringify(v));
test('UNIT_ONLY primary positional request preserves real business input and only server actor',()=>{
 assert.deepEqual(args,{p_contract_week_id:I.week,p_reason:payload.reason,p_expected_signature:payload.expected_signature,
 p_request_key:payload.request_key,p_actor_user_id:I.actor});
});
for(const key of ['actor_user_id','timesheet_id','source_cycle_id','source_cas','original_draft_invoice_id','schedule','phase','qualified']){
 test('UNIT_ONLY caller cannot select '+key,()=>assert.throws(()=>agencyBusinessRequest({...payload,[key]:'caller'},I.actor),e=>e.code==='AGENCY_CHANGE_BUSINESS_REQUEST_INVALID'));
}
for(const key of Object.keys(payload))test('UNIT_ONLY scalar input refuses array '+key,()=>
 assert.throws(()=>agencyBusinessRequest({...payload,[key]:[payload[key]]},I.actor),e=>e.code==='AGENCY_CHANGE_BUSINESS_REQUEST_INVALID'));
test('UNIT_ONLY actual native btrim admits tabs/NBSP without changing raw business bytes',()=>{
 const p={...payload,reason:'\tactual\u00a0',request_key:'\tkey\t'};const a=agencyBusinessRequest(p,I.actor);
 assert.equal(a.p_reason,p.reason);assert.equal(a.p_request_key,p.request_key);
 assert.throws(()=>agencyBusinessRequest({...p,request_key:' key'},I.actor));
});
test('UNIT_ONLY native character limit permits bounded Unicode input without UTF16 drift',()=>
 assert.equal(agencyBusinessRequest({...payload,reason:'😀'.repeat(2000)},I.actor).p_reason.length,4000));
test('UNIT_ONLY PostgreSQL UUID identity is not restricted to generated version bits',()=>
 assert.equal(agencyBusinessRequest({...payload,contract_week_id:'00000000-0000-0000-0000-000000000001'},I.actor).p_contract_week_id,'00000000-0000-0000-0000-000000000001'));
for(const [name,alter] of [
 ['foreign viewer',v=>{v.view_actor_user_id=I.other;}],['foreign week',v=>{v.contract_week_id=I.other;}],
 ['changed key',v=>{v.request_key+='other';}],['caller receipt field',v=>{v.qualified=true;}],
 ['completed phase only',v=>{v.phase='COMPLETED';}],['completed flag only',v=>{v.completed=true;}],
 ['fake queue success',v=>{v.code='OK';}],['array continuation',v=>{v.continuation=['MANUAL'];}],
 ['missing current signature',v=>{v.current_row_signature=null;}],['unsafe version',v=>{v.request_version=Number.MAX_SAFE_INTEGER+1;}],
 ['fake invoice descriptor',v=>{v.return_invoice_no='D1-unreturned';}],['actor completion before native finish',v=>{v.completed_actor_user_id=I.actor;}]
 ])test('UNIT_ONLY native response refuses '+name,()=>{const v=reply();alter(v);assert.throws(()=>decodeAgencyBusinessResult(v,args));});
test('UNIT_ONLY completed source result retains actual earlier phase actor on permitted replay',()=>{
 const v=reply('COMPLETED',10,{completed_actor_user_id:I.other});assert.equal(decodeAgencyBusinessResult(v,args).completed_actor_user_id,I.other);
});
test('UNIT_ONLY native target/hash cannot rotate between inline observations',()=>{
 const before=reply();for(const field of ['request_id','timesheet_id','request_sha256','timesheet_version']){
  const v=reply('RECOMPUTE_PENDING',5,{[field]:field==='request_sha256'?'c'.repeat(64):field==='timesheet_version'?4:I.other});
  assert.throws(()=>decodeAgencyBusinessResult(v,args,before),e=>e.code==='AGENCY_CHANGE_NATIVE_BUSINESS_TARGET_CHANGED');}
});
test('UNIT_ONLY increasing request version cannot conceal regressed native phase',()=>
 assert.throws(()=>decodeAgencyBusinessResult(reply('UNAUTHORISED',11),args,reply('AUTHORISED',9)),
  e=>e.code==='AGENCY_CHANGE_NATIVE_BUSINESS_TARGET_CHANGED'));
function runtime(sequence,options={}){
 const calls=[];let n=0;
 const run=createAgencyTimesheetChangeRuntime({nativeCommand:async a=>{calls.push(['native',copy(a)]);return copy(sequence[Math.min(n++,sequence.length-1)]);},
  runManual:async id=>{calls.push(['manual',id]);if(options.manualError)throw options.manualError;return options.manualResult;},
  runWorkerOnce:async id=>{calls.push(['worker',id]);if(options.workerError)throw options.workerError;return options.workerResult;}});
 return {calls,run:()=>run({request:payload,actor:{id:I.actor}})};
}
test('UNIT_ONLY actual runtime completes only after fresh native manual/queued/authorised/Return observations',async()=>{
 const c=runtime([reply(),reply('RECOMPUTE_PENDING',6),reply('AUTHORISED',9),reply('COMPLETED',12)]);
 const out=await c.run();assert.equal(out.result.completed,true);assert.equal(out.runtime_refusal_code,null);
 assert.deepEqual(c.calls.map(c=>c[0]),['native','manual','native','worker','native','worker','native']);
 assert.ok(c.calls.filter(c=>c[0]==='worker').every(c=>c[1]===I.ts));
});
test('UNIT_ONLY stored completed replay causes no calculator or Worker mutation',async()=>{
 const c=runtime([reply('COMPLETED',12)]);assert.equal((await c.run()).result.completed,true);assert.deepEqual(c.calls.map(c=>c[0]),['native']);
});
test('UNIT_ONLY queue success and completed target counts cannot mint business completion',async()=>{
 const c=runtime([reply('RECOMPUTE_PENDING',6)],{workerResult:{ok:1,completed_timesheet_ids:[I.ts]}});
 const out=await c.run();assert.equal(out.result.completed,false);assert.equal(out.result.code,'AGENCY_INVOICE_TSFIN_NOT_READY');
 assert.deepEqual(c.calls.map(c=>c[0]),['native','worker','native']);
});
test('UNIT_ONLY an existing live lease remains pending without forced enqueue',async()=>{
 const c=runtime([reply('RECOMPUTE_PENDING',6)],{workerResult:{picked:0,ok:0,fail:0}});
 const out=await c.run();assert.equal(out.result.phase,'RECOMPUTE_PENDING');assert.equal(c.calls.filter(c=>c[0]==='worker').length,1);
});
test('UNIT_ONLY manual transport loss keeps the actual stored phase and closed causal code',async()=>{
 const e=Object.assign(new Error('private frame must never reach product'),{json:{message:'AGENCY_CHANGE_NATIVE_OUTBOX_CHANGED',details:{private:'frame'}}});
 const c=runtime([reply(),reply('RECOMPUTE_PENDING',6)],{manualError:e});const out=await c.run();
 assert.equal(out.result.phase,'RECOMPUTE_PENDING');assert.equal(out.runtime_refusal_code,'AGENCY_CHANGE_NATIVE_OUTBOX_CHANGED');
 assert.equal(c.calls.filter(c=>c[0]==='worker').length,0);assert.ok(!JSON.stringify(out).includes('private'));
});
test('UNIT_ONLY transport failure after a native COMMIT does not relabel stored completion',async()=>{
 const c=runtime([reply('AUTHORISED',9),reply('COMPLETED',12)],{workerError:new Error('network lost after commit')});
 const out=await c.run();assert.equal(out.result.completed,true);assert.equal(out.runtime_refusal_code,null);
});
test('UNIT_ONLY native genuine held reason and return hold remain truthful',async()=>{
 const c=runtime([reply('DRAFT_REMOVED',5,{last_refusal_code:'AGENCY_CHANGE_NATIVE_PAY_IDENTITY_CHANGED',code:'AGENCY_CHANGE_NATIVE_PAY_IDENTITY_CHANGED'})]);
 const out=await c.run();assert.equal(out.result.return_hold,true);assert.equal(out.result.phase,'DRAFT_REMOVED');assert.equal(c.calls.length,1);
});
test('UNIT_ONLY arbitrarily increasing pending versions do not create an unbounded Worker loop',async()=>{
 const c=runtime([reply('RECOMPUTE_PENDING',6),reply('RECOMPUTE_PENDING',7),reply('RECOMPUTE_PENDING',8)]);
 const out=await c.run();assert.equal(out.result.completed,false);assert.equal(c.calls.filter(c=>c[0]==='worker').length,2);
});
test('UNIT_ONLY route projection rejects bare callback success and raw private context',()=>{
 assert.throws(()=>decodeAgencyInlineReply({result:{completed:true},runtime_refusal_code:null},args));
 assert.throws(()=>decodeAgencyInlineReply({result:reply(),runtime_refusal_code:null,private_context:{}},args));
});
const edges=JSON.parse(read('BUSINESS_ENTRY_CURRENT_REVERSES.json')).reverses;
for(const e of edges)test('UNIT_ONLY full current owner reverse '+e.output,()=>{
 let s=read(e.output);assert.equal(hash(s),e.forward_sha256);for(const h of [...e.hunks].reverse()){
  assert.equal(s.split(h.after).length,2,h.label);s=s.replace(h.after,()=>h.before);}
 assert.equal(hash(s),e.input_sha256);assert.equal(s,read('before/'+e.output+'.txt'));
});
test('UNIT_ONLY actual production mount executes correct named args and one physical target',async()=>{
 const s=read('81_complete_worker_index_with_one_click_agency_continuation.js'),a=s.indexOf('        orchestrateAgencyTimesheetChange: createAgencyTimesheetChangeRuntime({'),b=s.indexOf('        // WP-37',a);
 const calls=[];const context=vm.createContext({createAgencyTimesheetChangeRuntime:o=>o,env:{name:'UNIT_ONLY'},
  sbRpcRecordingGuardRefusal:async(...a)=>{calls.push(['recording',...a]);return {};},sbRpc:async(...a)=>{calls.push(['rpc',...a]);return {};},
  runAgencyCanonicalManual:async o=>{await o.readContext(o.requestId);await o.buildWeeklySnapshot('ts','cw','contract','fin',{write_now:false});await o.applySnapshot({native:'UNIT_ONLY'});},
  buildWeeklyScheduleSegmentsSnapshot:async(...a)=>{calls.push(['calculator',...a]);return {};},runTsfinWorkerOnce:async(...a)=>{calls.push(['worker',...a]);return {};}});
 vm.runInContext('this.factory=({'+s.slice(a,b)+'}).orchestrateAgencyTimesheetChange;',context);
 await context.factory.nativeCommand(args);await context.factory.runManual(I.request);await context.factory.runWorkerOnce(I.ts);
 const d=copy(calls);assert.equal(d[0][2],'weekly_source_agency_timesheet_change_v1');assert.deepEqual(d[0][3],args);
 assert.equal(d[1][2],'weekly_source_agency_change_worker_context_v1');assert.deepEqual(d[1][3],{p_request_id:I.request});
 assert.equal(d[2][0],'calculator');assert.equal(d[3][2],'weekly_source_agency_change_worker_apply_v1');
 assert.deepEqual(d[4][2],{limit:1,onlyTimesheetIds:[I.ts]});
});
test('UNIT_ONLY native business owner retains exact one request and actual receipt phases',()=>{
 const s=read('80_native_agency_business_entry.sqlinc');
 assert.match(s,/weekly_source_agency_timesheet_change_v1\(\s*p_contract_week_id uuid,p_reason text,p_expected_signature text,p_request_key text,p_actor_user_id uuid/);
 assert.match(s,/weekly_source_agency_change_begin_v1\(p_contract_week_id,p_reason/);
 assert.match(s,/r\.phase='FINANCIAL_CURRENT'[\s\S]*weekly_source_agency_change_authorise_step_v1/);
 assert.match(s,/r\.phase='AUTHORISED'[\s\S]*weekly_source_agency_change_return_step_v1/);
 assert.match(s,/WEEKLY_SOURCE_AGENCY_NATIVE_COMPLETED_V1/);assert.match(s,/completed_receipt->>'request_sha256'/);
 assert.doesNotMatch(s,/insert into|update public\.timesheets|enqueue_ts|set_config|qualified.*true|create table/i);
});
function actualRoute(actor,returned,body={action:'AGENCY_TIMESHEET_CHANGE',payload},environment='TEST'){
 const source=read('82_complete_current_routes_with_one_click_agency_command.js')
  .replace(/import\s+[\s\S]*?\s+from\s+['"][^'"]+['"];\r?\n/g,'')
  .replace(/\bexport (?=class|async function|function|const)/g,'');
 const calls=[];const sandbox={Object,Number,String,Array,Set,Map,TextEncoder,Response,URL,JSON,Date,RegExp,
  isActivePaymentActor,agencyBusinessRequest,decodeAgencyInlineReply,bpayBusyEnvelope:()=>null};
 vm.createContext(sandbox);vm.runInContext(source+'\nthis.dispatch=dispatchWeeklySourceRequest;',sandbox);
 const req=new Request('https://unit.invalid/api/weekly-source/v1/commands',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
 return {calls,run:()=>sandbox.dispatch(req,{CANDIDATE_APP_ENVIRONMENT:environment,MYTMS_OFFICE_AGENCY_ID:u(19)}, {},{
  requireUser:async(_env,_req,roles)=>{calls.push({kind:'actor',roles:copy(roles)});return actor;},
  orchestrateAgencyTimesheetChange:async details=>{calls.push({kind:'native',details:copy(details)});return copy(returned);}})};
}
for(const [name,actor] of [['admin',{id:I.actor,is_active:true,role:'admin'}],
 ['current pay authoriser',{id:I.actor,is_active:true,role:'staff',payment_authoriser:true}],
 ['current golden key',{id:I.actor,is_active:true,role:'staff',payment_golden_key:true}]]){
 test('UNIT_ONLY actual whole command/actor caller permits '+name+' only through native entry',async()=>{
  const c=actualRoute(actor,{result:reply('COMPLETED',12),runtime_refusal_code:null});const r=await c.run(),v=await r.json();
  assert.equal(r.status,200);assert.equal(v.ok,true);assert.equal(c.calls[1].details.actor.id,I.actor);
  assert.deepEqual(c.calls[1].details.request,payload);assert.deepEqual(c.calls[0].roles,[]);
 });}
for(const actor of [{id:I.actor,is_active:false,role:'admin'},
 {id:I.actor,is_active:true,role:'staff',payment_authoriser:false,payment_golden_key:false}]){
 test('UNIT_ONLY actual whole command denies inactive/unscoped actor before native mutation '+JSON.stringify(actor),async()=>{
  const c=actualRoute(actor,{result:reply('COMPLETED',12),runtime_refusal_code:null});const r=await c.run();
  assert.ok([401,403].includes(r.status));assert.equal(c.calls.filter(c=>c.kind==='native').length,0);
 });}
test('UNIT_ONLY actual whole command preserves unfinished native phase with HTTP409',async()=>{
 const c=actualRoute({id:I.actor,is_active:true,role:'admin'},{result:reply('RECOMPUTE_PENDING',7),runtime_refusal_code:null});
 const r=await c.run(),v=await r.json();assert.equal(r.status,409);assert.equal(v.ok,false);assert.equal(v.result.phase,'RECOMPUTE_PENDING');
});
test('UNIT_ONLY actual whole command rejects fake native completion and private callback frames',async()=>{
 const c=actualRoute({id:I.actor,is_active:true,role:'admin'},{result:{completed:true},runtime_refusal_code:null,private_frame:{}});
 const r=await c.run(),v=await r.json();assert.equal(r.status,503);assert.equal(v.error_code,'AGENCY_CHANGE_NATIVE_BUSINESS_RESULT_INVALID');
 assert.ok(!JSON.stringify(v).includes('private_frame'));
});
test('UNIT_ONLY actual whole command refuses LIVE deployment before action',async()=>{
 const c=actualRoute({id:I.actor,is_active:true,role:'admin'},{result:reply('COMPLETED',12),runtime_refusal_code:null},undefined,'LIVE');
 const r=await c.run();assert.equal(r.status,403);assert.equal(c.calls.filter(c=>c.kind==='native').length,0);
});
test('UNIT_ONLY actual whole command rejects browser actor replacement before action',async()=>{
 const c=actualRoute({id:I.actor,is_active:true,role:'admin'},{result:reply(),runtime_refusal_code:null},
  {action:'AGENCY_TIMESHEET_CHANGE',payload:{...payload,actor_user_id:I.other}});
 const r=await c.run();assert.equal(r.status,400);assert.equal(c.calls.filter(c=>c.kind==='native').length,0);
});
function manualFrame(kind){const shift={date:'2026-10-09',start:'09:00',end:'17:00',end_date:'2026-10-09',break_minutes:30,source_shift_id:u(10),work_event_id:u(10)};
 return {schema_version:'WEEKLY_SOURCE_AGENCY_CANONICAL_WORKER_CONTEXT_V1',request_id:I.request,request_sha256:'a'.repeat(64),request_version:4,
 phase:'UNAUTHORISED',phase_actor_user_id:I.actor,current_row_signature:'UNIT_ONLY_NATIVE_SIGNATURE',
 source_cas:{schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:u(11),client_ids:[u(12)],source_cas_sha256:'b'.repeat(64)},
 source_schedule:[shift],outbox_id:null,outbox_reason:null,outbox_row:null,
 context:{effective_timesheet_id:I.ts,out_timesheet:{timesheet_id:I.ts,contract_id:u(13),version:3,sheet_scope:'WEEKLY',line_type:'HOURS',
 is_current:true,authorised_at_server:null},out_cur_fin:{id:u(14),timesheet_id:I.ts,timesheet_version:3,locked_by_invoice_id:null},
 out_candidate:{id:u(15)},out_umbrella:null,out_client_id:u(12),out_effective_flags:{},out_policy:{unit_only:true}},
 weekly_context:{timesheet_id:I.ts,out_cw:{id:I.week,timesheet_id:I.ts,contract_id:u(13)},out_contract:{id:u(13),candidate_id:u(15),client_id:u(12)}},
 weekly_validation:{timesheet_id:I.ts,status:'VALIDATION_ERROR',reason_code:'HOURS_DIFFER',validated_at_utc:'2026-10-14T12:00:00Z',
 updated_at:'2026-10-14T12:00:00Z',validation_source_kind:kind},nhsp_shifts:[],context_sha256:'c'.repeat(64)};}
for(const kind of ['LEGACY','AGENCY_INVOICE_WEEK'])test('UNIT_ONLY actual MANUAL context/codec/options carries stored '+kind+' marker',async()=>{
 const f=manualFrame(kind);let called=0;
 await runAgencyCanonicalManual({requestId:I.request,readContext:async()=>copy(f),buildWeeklySnapshot:async(ts,cw,contract,fin,options)=>{
  called++;assert.equal(options.hr_validation_source_kind,kind);assert.equal(options.hr_validation_status,'VALIDATION_ERROR');
  return {ok:true,snapshot:{timesheet_id:I.ts,timesheet_version:3,candidate_id:u(15),client_id:u(12),invoice_breakdown_json:{mode:'SEGMENTS',segments:[
   {segment_id:'UNIT_ONLY_SEGMENT',date:'2026-10-09',start:'09:00',end:'17:00',break_mins:30}]}}};},
 applySnapshot:async()=>({schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_WORKER_RESULT_V1',request_id:I.request,request_version:6,phase:'RECOMPUTE_PENDING',
  phase_actor_user_id:I.actor,timesheet_id:I.ts,timesheet_version:3,current_financial_id:u(16),outbox_id:u(17),last_refusal_code:null,
  native_write_observed:true,native_queue_completed:false})});assert.equal(called,1);
});
test('UNIT_ONLY actual MANUAL context refuses an absent or malformed typed marker before calculator',async()=>{
 for(const kind of [undefined,null,['AGENCY_INVOICE_WEEK'],'CALLER_KIND']){const f=manualFrame(kind);let calls=0;
  await assert.rejects(runAgencyCanonicalManual({requestId:I.request,readContext:async()=>copy(f),buildWeeklySnapshot:async()=>{calls++;},applySnapshot:async()=>{}}));
  assert.equal(calls,0);}
});
