import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
import {fileURLToPath} from 'node:url';import path from 'node:path';import crypto from 'node:crypto';
import {decodeAgencyAuthorisedValidationContext,runAgencyLeasedAuthorisedValidation,agencyNativeCausalCode} from './69_complete_canonical_worker_with_authorised_validation.mjs';
import {decodeAgencyReturnStep} from './agency-native-return.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),read=p=>fs.readFileSync(path.join(here,p),'utf8'),copy=x=>JSON.parse(JSON.stringify(x));
const u=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,stamp='2026-10-14T12:00:00+00:00';
const e={date:'2026-10-09',start:'09:00',end:'17:00',end_date:'2026-10-09',break_minutes:30,source_shift_id:u(10),work_event_id:u(10)};
function frame(complete=true){return {schema_version:'WEEKLY_SOURCE_AGENCY_AUTHORISED_VALIDATION_CONTEXT_V1',request_id:u(1),request_sha256:'a'.repeat(64),
 request_version:20,phase:'AUTHORISED',phase_actor_user_id:u(8),current_row_signature:'UNIT_ONLY_NATIVE_SIGNATURE',
 source_cas:{schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:u(9),client_ids:[u(7)],source_cas_sha256:'b'.repeat(64)},
 source_schedule:[copy(e)],outbox_id:u(11),outbox_reason:'CONTEXT_CHANGED',outbox_row:{id:u(11),timesheet_id:u(2),reason:'CONTEXT_CHANGED',
  attempt_count:1,next_attempt_at:'2026-10-14T12:05:00Z',last_error:null,created_at:stamp},
 context:{effective_timesheet_id:u(2),out_timesheet:{timesheet_id:u(2),contract_id:u(5),version:1,sheet_scope:'WEEKLY',line_type:'HOURS',
  is_current:true,authorised_at_server:stamp,revoked_at:null,actual_schedule_json:[copy(e)]},out_cur_fin:{id:u(3),timesheet_id:u(2),timesheet_version:1,
  locked_by_invoice_id:null,paid_at_utc:null,authorised_at_utc:stamp,authorised_by_user_id:u(8),actual_schedule_json:[copy(e)]},out_candidate:{id:u(6)},
  out_umbrella:null,out_client_id:u(7),out_effective_flags:{},out_policy:{unit_only:true}},
 weekly_context:{timesheet_id:u(2),out_cw:{id:u(4),timesheet_id:u(2),contract_id:u(5)},out_contract:{id:u(5),candidate_id:u(6),client_id:u(7)}},
 weekly_validation:{id:u(16),timesheet_id:u(2),booking_id:'UNIT_ONLY_NATIVE_BOOKING',status:complete?'VALIDATION_OK':'VALIDATION_ERROR',reason_code:complete?null:'HOURS_DIFFER',
  hr_request_id:null,validated_at_utc:stamp,override_requested_at_utc:null,override_requested_by:null,override_confirmed_at_utc:null,override_confirmed_by:null,
  override_reason:null,override_cleared_at_utc:null,last_source:null,created_at:stamp,updated_at:stamp,hr_request_source:null,hr_request_set_by:null,
  hr_request_set_at_utc:null,pre_validated:false,validation_source_kind:'AGENCY_INVOICE_WEEK',validation_source_publication_id:u(17),
  validation_source_facts_sha256:'\\x'+'d'.repeat(64),validation_source_actor_user_id:u(8)},nhsp_shifts:[],week_facts_sha256:'d'.repeat(64),week_complete:complete,
 validation_actor_user_id:u(8),context_sha256:'c'.repeat(64)};}
function reply(f=frame()){return {schema_version:'WEEKLY_SOURCE_AGENCY_VALIDATION_APPLY_RESULT_V1',request_id:u(1),request_version:24,phase:'AUTHORISED',
 phase_actor_user_id:u(8),timesheet_id:u(2),timesheet_version:1,outbox_id:u(11),financial_id:u(13),native_write_observed:true,native_queue_completed:true,
 validation_receipt:{schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_VALIDATION_RECOMPUTE_V1',request_id:u(1),timesheet_id:u(2),timesheet_version:1,
 financial_id:u(13),prior_financial_id:u(3),observed_outbox_id:u(11),publication_id:u(17),facts_sha256:f.week_facts_sha256,
 validation_actor_user_id:u(8),phase_actor_user_id:u(8),financial_row_sha256:'e'.repeat(64),transaction_id:'123456',backend_pid:1234,native_ok_count:1,native_fail_count:0}};}
function options(f=frame(),result=reply(f)){const calls=[];return {calls,requestId:u(1),leasedRows:[{outbox_id:u(11),timesheet_id:u(2),reason:'CONTEXT_CHANGED'}],
 readContext:async()=>{calls.push('READ');return copy(f);},buildWeeklySnapshot:async(t,cw,con,cur,opt)=>{calls.push({kind:'BUILD',t,opt});return{ok:true,snapshot:{
  timesheet_id:u(2),timesheet_version:1,candidate_id:u(6),client_id:u(7),invoice_breakdown_json:{mode:'SEGMENTS',segments:[{
   segment_id:'UNIT_ONLY_NATIVE_COMPONENT',date:e.date,start:e.start,end:e.end,break_mins:30,is_reversal:false,pay_amount:123,charge_amount:234}]}}};},
 applySnapshot:async args=>{calls.push({kind:'APPLY',args});return copy(result);}};}
test('actual authorised mode retains real approval and uses same canonical calculator/native apply',async()=>{const o=options();const r=await runAgencyLeasedAuthorisedValidation(o);
 assert.equal(r.phase,'AUTHORISED');assert.equal(r.native_queue_completed,true);assert.equal(o.calls[1].t.authorised_at_server,stamp);
 assert.equal(o.calls[1].opt.hr_validation_status,'VALIDATION_OK');assert.equal(o.calls[2].args.p_snapshot.invoice_breakdown_json.segments[0].work_event_id,u(10));});
test('actual incomplete native week can recompute READY_FOR_HR without claiming Return/completion',async()=>{const o=options(frame(false));const r=await runAgencyLeasedAuthorisedValidation(o);
 assert.equal(r.phase,'AUTHORISED');assert.equal(o.calls[1].opt.hr_validation_status,'VALIDATION_ERROR');assert.equal(r.completed,undefined);});
for(const [name,change] of [
 ['UNA label',x=>x.phase='UNAUTHORISED'],['missing real approval',x=>x.context.out_timesheet.authorised_at_server=null],
 ['foreign approval time',x=>x.context.out_cur_fin.authorised_at_utc='2026-10-15T00:00:00Z'],['paid current writer target',x=>x.context.out_cur_fin.paid_at_utc=stamp],
 ['D1 locked current writer target',x=>x.context.out_cur_fin.locked_by_invoice_id=u(99)],['foreign current financial',x=>x.context.out_cur_fin.timesheet_id=u(99)],
 ['false complete annotation',x=>x.week_complete=true&&x.weekly_validation.status==='VALIDATION_ERROR'],['import UUID shortcut',x=>x.weekly_validation.last_source=u(99)],
 ['legacy marker',x=>x.weekly_validation.validation_source_kind='LEGACY'],['override status',x=>x.weekly_validation.status='OVERRIDDEN'],
 ['wrong typed facts',x=>x.weekly_validation.validation_source_facts_sha256='\\x'+'f'.repeat(64)],['wrong validating actor',x=>x.validation_actor_user_id=u(99)],
 ['wrong queue reason',x=>x.outbox_reason='MANUAL'],['wrong queue target',x=>x.outbox_row.timesheet_id=u(99)],['unknown authority field',x=>x.qualified=true],
 ['array actor',x=>x.phase_actor_user_id=[u(8)]],['array date',x=>x.source_schedule[0].date=[e.date]],['array time',x=>x.source_schedule[0].start=[e.start]],
 ['unbound source identity',x=>x.source_schedule[0].source_shift_id=u(99)],['asserted validation actor',x=>x.weekly_validation.override_requested_by=u(8)]
])test(`native context refusal before calculator: ${name}`,async()=>{const f=frame();change(f);const o=options(f);
 await assert.rejects(runAgencyLeasedAuthorisedValidation(o),/AUTHORISED_VALIDATION_CONTEXT_INVALID/);assert.deepEqual(o.calls,['READ']);});
test('foreign leased outbox cannot call calculator/native apply',async()=>{const o=options();o.leasedRows[0].outbox_id=u(99);
 await assert.rejects(runAgencyLeasedAuthorisedValidation(o),/NATIVE_OUTBOX_NOT_LEASED/);assert.deepEqual(o.calls,['READ']);});
for(const [name,change] of [['bare queue success',x=>x.validation_receipt={}],['same current row',x=>x.financial_id=u(3)],
 ['wrong native count',x=>x.validation_receipt.native_ok_count=0],['wrong observed queue',x=>x.validation_receipt.observed_outbox_id=u(99)],
 ['wrong genuine publication',x=>x.validation_receipt.publication_id=u(99)],['wrong original financial',x=>x.validation_receipt.prior_financial_id=u(99)],
 ['fake final phase',x=>x.phase='COMPLETED'],['foreign actual actor',x=>x.validation_receipt.phase_actor_user_id=u(99)]])
 test(`native result refusal: ${name}`,async()=>{const r=reply();change(r);await assert.rejects(runAgencyLeasedAuthorisedValidation(options(frame(),r)),/NATIVE_VALIDATION_RESULT_UNCONFIRMED/);});
const index=read('68_complete_worker_index_with_native_return.js'),begin=index.indexOf('    const agencyTarget = agencyWorkerTargets.get(effId);'),end=index.indexOf('    const ctx = ctxByEff.get(effId) || null;',begin),branch=index.slice(begin,end);
assert.ok(begin>=0&&end>begin);
async function caller({queueFailure=false,returnFailure=false,returnReply}={}){const calls=[],warnings=[],completedPrimaryOutboxSet=new Set(),completedTimesheetIds=new Set();
 const context={console:{warn:(...x)=>warnings.push(x)},agencyWorkerTargets:new Map([[u(2),{request_id:u(1),phase:'AUTHORISED'}]]),effId:u(2),
  lease:[{outbox_id:u(11),timesheet_id:u(2),reason:'CONTEXT_CHANGED'}],outboxIds:[u(11)],env:{},completedPrimaryOutboxSet,completedTimesheetIds,
  runAgencyLeasedAuthorisedValidation:async()=>{calls.push('VALIDATION');if(queueFailure)throw Object.assign(new Error('UNIT_ONLY_NATIVE_QUEUE_FAILURE'),{code:'UNIT_ONLY_NATIVE_QUEUE_FAILURE'});return reply();},
  runAgencyLeasedCanonicalRecompute:()=>{throw new Error('WRONG_UNA_MODE');},decodeAgencyAuthoriseStepResult:()=>{throw new Error('WRONG_AUTHORISE_PHASE');},
  agencyNativeCausalCode,decodeAgencyReturnStep:(v,e)=>decodeAgencyReturnStep(copy(v),copy(e)),sbRpc:async(_env,name,args)=>{calls.push({name,args});},
  sbRpcRecordingGuardRefusal:async(_env,name,args)=>{calls.push({name,args});if(returnFailure)throw Object.assign(new Error('RAW_PRIVATE_FRAME'),{code:'AGENCY_CHANGE_CURRENT_NATIVE_WEEK_VALIDATION_REQUIRED'});
   return returnReply??{schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_RETURN_STEP_RESULT_V1',request_id:u(1),request_version:25,phase:'COMPLETED',
    view_actor_user_id:u(8),timesheet_id:u(2),timesheet_version:1,completed:true,last_refusal_code:null,completed_actor_user_id:u(8),return_invoice_no:'UNIT_ONLY_D1',completion_sha256:'f'.repeat(64)};},
  buildWeeklyScheduleSegmentsSnapshot:()=>{throw new Error('UNIT_ONLY_STUBBED_NATIVE_OUTCOME');}};
 const fn=vm.runInNewContext('(async function(){let ok=0,fail=0;for(const iteration of [1]){'+branch+'}return {ok,fail};})',context);
 const result=await fn();return{result:copy(result),calls:copy(calls),warnings:copy(warnings),completedPrimaryOutboxSet};}
test('actual whole Worker caller chooses authorised mode then literal Return with native actor',async()=>{const r=await caller();assert.deepEqual(r.result,{ok:1,fail:0});
 assert.deepEqual(r.calls,['VALIDATION',{name:'weekly_source_agency_change_return_step_v1',args:{p_request_id:u(1),p_actor_user_id:u(8)}}]);assert.equal(r.completedPrimaryOutboxSet.has(u(11)),true);});
test('failed native queue never invokes Return',async()=>{const r=await caller({queueFailure:true});assert.deepEqual(r.result,{ok:0,fail:1});
 assert.equal(r.calls.some(x=>x.name==='weekly_source_agency_change_return_step_v1'),false);assert.equal(r.completedPrimaryOutboxSet.size,0);});
test('late genuine Return hold never relabels/deletes completed queue or logs raw frame',async()=>{const r=await caller({returnFailure:true});assert.deepEqual(r.result,{ok:1,fail:0});
 assert.equal(r.calls.some(x=>x.name==='tsfin_work_fail'),false);assert.equal(r.completedPrimaryOutboxSet.has(u(11)),true);
 assert.match(JSON.stringify(r.warnings),/CURRENT_NATIVE_WEEK_VALIDATION_REQUIRED/);assert.doesNotMatch(JSON.stringify(r.warnings),/RAW_PRIVATE_FRAME/);});
test('malformed late Return reply keeps actual queue completion',async()=>{const r=await caller({returnReply:{}});assert.deepEqual(r.result,{ok:1,fail:0});
 assert.equal(r.calls.some(x=>x.name==='tsfin_work_fail'),false);assert.match(JSON.stringify(r.warnings),/NATIVE_RETURN_RESULT_UNCONFIRMED/);});
test('all unique runtime/native reversals restore actual accepted predecessors',()=>{const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const name of ['AUTHORISED_NATIVE_FULL_REVERSE.json','AUTHORISED_RUNTIME_FULL_REVERSE.json']){const reverse=JSON.parse(read(name));for(const j of reverse.joins){let s=read(j.output);assert.equal(sha(s),j.after_sha256);
  for(const h of [...j.hunks].reverse()){if(h.before===''){assert.equal(s.slice(h.offset),h.after);s=s.slice(0,h.offset);}else{assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}}
  assert.equal(sha(s),j.before_sha256);}}});

const weekly=index.slice(index.indexOf('async function buildWeeklyScheduleSegmentsSnapshot('));
const statusStart=weekly.indexOf('  const valStatusU ='),statusEnd=weekly.indexOf('  const parseUtcMs =',statusStart);
assert.ok(statusStart>=0&&statusEnd>statusStart);
const actualStatus=weekly.slice(statusStart,statusEnd);
for(const [name,kind,status,reason,required,expected] of [
 ['typed different hours','AGENCY_INVOICE_WEEK','VALIDATION_ERROR','HOURS_DIFFER',true,'READY_FOR_HR'],
 ['typed coverage missing','AGENCY_INVOICE_WEEK','VALIDATION_ERROR','COVERAGE_MISSING',true,'READY_FOR_HR'],
 ['typed native complete','AGENCY_INVOICE_WEEK','VALIDATION_OK',null,true,'READY_FOR_INVOICE'],
 ['typed override is not native completeness','AGENCY_INVOICE_WEEK','OVERRIDDEN',null,true,'READY_FOR_HR'],
 ['ordinary legacy override',null,'OVERRIDDEN',null,true,'READY_FOR_INVOICE'],
 ['ordinary legacy named failure',null,'VALIDATION_ERROR','HEALTHROSTER_WEEKLY_HOURS_DIFFER',true,'READY_FOR_HR'],
 ['ordinary nonweekly reason',null,'VALIDATION_ERROR','HOURS_DIFFER',true,'READY_FOR_INVOICE']
])test(`actual canonical readiness branch: ${name}`,()=>{
 const f=vm.runInNewContext('(function(){let processing_status;'+actualStatus+'return processing_status;})',{
  hr_validation_status:status,hr_validation_reason_code:reason,hr_validation_source_kind:kind,
  hrValidationRequired:required,qrAwaitingSignatureUpload:false,isAuthorised:true});assert.equal(f(),expected);});
const hBegin=index.indexOf('async function handleHRValidate('),hEnd=index.indexOf('// ---------------------- Router wiring hint',hBegin);
assert.ok(hBegin>=0&&hEnd>hBegin);const handler=index.slice(hBegin,hEnd);
async function legacy({user={id:u(8)},admitted=true,error}={}){const calls=[];
 const context={console,agencyNativeCausalCode,requireUser:async(_env,_req,roles)=>{calls.push({name:'USER',roles:copy(roles)});return user;},
  sbRpcRecordingGuardRefusal:async(_env,name,args)=>{calls.push({name,args:copy(args)});if(error)throw error;return admitted;},
  sbFetch:async()=>{calls.push({name:'LOAD'});return{rows:[{parse_summary_json:{unit_only:true}}]};},
  fetch:async(url,options)=>{calls.push({name:'FETCH',method:options.method,url});return{ok:true};},sbHeaders:()=>({}),writeAudit:async()=>calls.push({name:'AUDIT'}),
  withCORS:(_env,_req,res)=>res,unauthorized:()=>({status:401}),serverError:message=>({status:503,message}),
  json:(body,status)=>({status,body}),ok:body=>({status:200,body})};
 const fn=vm.runInNewContext(handler+';handleHRValidate',context);const result=await fn({SUPABASE_URL:'https://unit.invalid',TSO_WEBHOOK_MAP:JSON.stringify({hr_validate:'https://unit.invalid/hook'})},{},u(22));
 return{result:copy(result),calls:copy(calls)};}
test('actual legacy handler refuses preset before any import patch/provider webhook',async()=>{const r=await legacy({error:{json:{code:'55000',message:'AGENCY_INVOICE_USES_COMPLETENESS_WRITER',details:'UNIT_ONLY_PRIVATE_FRAME'}}});
 assert.equal(r.result.status,409);assert.equal(r.calls.some(x=>x.name==='FETCH'),false);assert.equal(r.calls.some(x=>x.name==='LOAD'),false);
 assert.doesNotMatch(JSON.stringify(r.result),/PRIVATE_FRAME/);assert.equal(r.calls[1].args.p_actor_user_id,u(8));});
test('actual legacy handler false/unknown admission is a dependency refusal before side effects',async()=>{const r=await legacy({admitted:false});
 assert.equal(r.result.status,503);assert.equal(r.calls.some(x=>x.name==='FETCH'),false);});
test('actual legacy handler retains ordinary admitted flow',async()=>{const r=await legacy();assert.equal(r.result.status,200);
 assert.deepEqual(r.calls.filter(x=>x.name==='FETCH').map(x=>x.method),['PATCH','POST']);assert.equal(r.calls[1].name,'weekly_source_agency_legacy_import_validation_admit_v1');});
test('actual unauthenticated legacy handler invokes no native admission/mutation',async()=>{const r=await legacy({user:null});assert.equal(r.result.status,401);assert.equal(r.calls.length,1);});
for(const value of ['raw private body',{message:'Contains private business details'},['AGENCY_INVOICE_USES_COMPLETENESS_WRITER']])
 test('causal parser refuses a non-scalar/non-code PostgREST message',()=>assert.equal(agencyNativeCausalCode({json:{message:value}},'UNIT_ONLY_DEPENDENCY'),'UNIT_ONLY_DEPENDENCY'));
test('causal parser matches actual sbRpc parsed PostgREST error shape',()=>assert.equal(agencyNativeCausalCode({status:409,code:undefined,
 json:{code:'55000',message:'AGENCY_CHANGE_ORIGINAL_SOURCE_CAS_CHANGED',details:'UNIT_ONLY_RAW_FRAME'}},'UNIT_ONLY_DEPENDENCY'),'AGENCY_CHANGE_ORIGINAL_SOURCE_CAS_CHANGED'));
test('current publisher and Mode A complete forward reversals preserve actual native before bodies',()=>{const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
 const r=JSON.parse(read('PUBLISHER_MODE_A_FULL_REVERSE.json'));for(const j of r.joins){let s=read(j.output);assert.equal(sha(s),j.after_sha256);
  for(const h of [...j.hunks].reverse()){if(h.offset!==undefined){assert.equal(s.slice(h.offset,h.offset+h.after.length),h.after);s=s.slice(0,h.offset)+h.before+s.slice(h.offset+h.after.length);}else{assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}}
  assert.equal(s,read(j.before_file));assert.equal(sha(s),j.before_sha256);}});
