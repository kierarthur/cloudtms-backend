import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import vm from 'node:vm';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
import {decodeAgencyAuthoriseStepResult} from './48_agency_canonical_worker_with_authorise_codec.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url));const u=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');const read=p=>fs.readFileSync(path.join(dir,p),'utf8');
const prior=()=>({schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_WORKER_RESULT_V1',request_id:u(1),request_version:9,phase:'FINANCIAL_CURRENT',
 phase_actor_user_id:u(2),timesheet_id:u(3),timesheet_version:1,current_financial_id:u(4),outbox_id:u(5),last_refusal_code:null,
 native_write_observed:true,native_queue_completed:true});
const when='2026-10-14T12:00:00+00:00';
function reply(){return{schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_AUTHORISE_STEP_RESULT_V1',request_id:u(1),request_version:13,phase:'AUTHORISED',
 view_actor_user_id:u(2),timesheet_id:u(3),timesheet_version:1,financial_id:u(4),last_refusal_code:null,completed_receipt:null,
 authorise_receipt:{schema_version:'WEEKLY_SOURCE_AGENCY_NATIVE_AUTHORISE_V1',request_id:u(1),phase_actor_user_id:u(2),timesheet_id:u(3),
 timesheet_version:1,financial_id:u(4),authorised_at_server:when,pay_purpose_sha256:'a'.repeat(64),transaction_id:'9107',backend_pid:1234,
 result:{ok:true,success:true,operation:'timesheet_authorise',timesheet_id:u(3),current_timesheet_id:u(3),requested_timesheet_id:u(3),
  contract_week_id:u(6),booking_id:'UNIT_ONLY_NATIVE_BOOKING',candidate_id:u(7),authorised_at_server:when,revoked_at:null,authorised_at_utc:when,
  advance_state_refresh:{},current_version:1,was_stale:false,previous_processing_status:'PENDING_AUTH',processing_status:'READY_FOR_HR',
  new_processing_status:'READY_FOR_HR',validation_status:'PENDING',validation_pre_validated:false,hr_validation_required_for_invoice:true,
  backend_row_signature:'UNIT_ONLY_NATIVE_SIGNATURE',row_signature:'UNIT_ONLY_NATIVE_SIGNATURE',
  affected_rows:[{timesheet_id:u(3)}],cache_invalidation_hints:{}}}};}
const held=()=>({...reply(),phase:'FINANCIAL_CURRENT',authorise_receipt:null,last_refusal_code:'AGENCY_INVOICE_TSFIN_NOT_READY'});
test('closed native Authorise reply retains READY_FOR_HR without claiming validation/Return',()=>{
 const result=decodeAgencyAuthoriseStepResult(reply(),prior());assert.equal(result.phase,'AUTHORISED');assert.equal(result.completed_receipt,null);
 assert.equal(result.authorise_receipt.result.processing_status,'READY_FOR_HR');});
test('genuine causal FINANCIAL_CURRENT hold remains a hold',()=>assert.deepEqual(decodeAgencyAuthoriseStepResult(held(),prior()),held()));
test('replay preserves real approval actor separately from current admitted reader',()=>{
 const value=reply();value.authorise_receipt.phase_actor_user_id=u(8);const decoded=decodeAgencyAuthoriseStepResult(value,prior());
 assert.equal(decoded.view_actor_user_id,u(2));assert.equal(decoded.authorise_receipt.phase_actor_user_id,u(8));});
for(const [name,mutate]of[
 ['foreign request',x=>x.request_id=u(90)],['foreign physical target',x=>x.timesheet_id=u(90)],
 ['foreign current reader',x=>x.view_actor_user_id=u(90)],['rotated version',x=>x.timesheet_version=2],
 ['foreign financial row',x=>x.financial_id=u(90)],['unsupported COMPLETE label',x=>x.phase='COMPLETED'],
 ['fake completion receipt',x=>x.completed_receipt={}],['raw extra key',x=>x.scope={}],
 ['false native success',x=>x.authorise_receipt.result.ok=false],['stale native target',x=>x.authorise_receipt.result.was_stale=true],
 ['wrong native status',x=>x.authorise_receipt.result.processing_status='PAID'],['array actor',x=>x.authorise_receipt.phase_actor_user_id=[u(2)]],
 ['fake native result object',x=>x.authorise_receipt.result={}],['wrong native operation',x=>x.authorise_receipt.result.operation='other'],
 ['wrong native signature',x=>x.authorise_receipt.result.row_signature='FOREIGN'],['array time',x=>x.authorise_receipt.authorised_at_server=[when]],
 ['invalid current XID type',x=>x.authorise_receipt.transaction_id=9107],['invalid PID',x=>x.authorise_receipt.backend_pid=0],
 ['hold relabelled successful',x=>{Object.assign(x,held());x.last_refusal_code=null;}],
 ['hold with a native approval',x=>{x.phase='FINANCIAL_CURRENT';x.last_refusal_code='AGENCY_INVOICE_TSFIN_NOT_READY';}],
])test(`actual decoder refuses ${name}`,()=>{const value=reply();mutate(value);
 assert.throws(()=>decodeAgencyAuthoriseStepResult(value,prior()),/NATIVE_AUTHORISE_RESULT_UNCONFIRMED/);});

const index=read('49_complete_worker_index_with_native_authorise.js');
const begin=index.indexOf('    const agencyTarget = agencyWorkerTargets.get(effId);');
const end=index.indexOf('    const ctx = ctxByEff.get(effId) || null;',begin);assert.ok(begin>=0&&end>begin);
const actualBranch=index.slice(begin,end);
async function runCaller({nativeReply=reply(),nativeFailure=false,queueFailure=false}={}){
 const calls=[],warnings=[],completedPrimaryOutboxSet=new Set(),completedTimesheetIds=new Set();
 const context={console:{warn:(...args)=>warnings.push(args)},agencyWorkerTargets:new Map([[u(3),{request_id:u(1)}]]),effId:u(3),
  lease:[{outbox_id:u(5),timesheet_id:u(3),reason:'MANUAL'}],outboxIds:[u(5)],env:{},completedPrimaryOutboxSet,completedTimesheetIds,
  decodeAgencyAuthoriseStepResult,runAgencyLeasedCanonicalRecompute:async()=>{if(queueFailure)throw Object.assign(new Error('UNIT_ONLY_QUEUE_FAILURE'),{code:'UNIT_ONLY_QUEUE_FAILURE'});return prior();},
  sbRpc:async(_env,name,args)=>{calls.push({name,args});},
  sbRpcRecordingGuardRefusal:async(_env,name,args)=>{calls.push({name,args});if(nativeFailure)throw Object.assign(new Error('PRIVATE_RAW_ERROR_MUST_NOT_PRINT'),{code:'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED'});return nativeReply;},
  buildWeeklyScheduleSegmentsSnapshot:()=>{throw new Error('STUBBED_NATIVE_RESULT_CASE_NO_CALC');}};
 const fn=vm.runInNewContext('(async function(){let ok=0,fail=0;for(const iteration of [1]){'+actualBranch+'}return {ok,fail};})',context);
 const result=await fn();return{result:{ok:result.ok,fail:result.fail},calls,warnings,completedPrimaryOutboxSet,completedTimesheetIds};
}
test('actual whole caller uses literal Authorise RPC after genuine queue receipt and exact native actor',async()=>{
 const r=await runCaller();assert.deepEqual(JSON.parse(JSON.stringify(r.calls)),[{name:'weekly_source_agency_change_authorise_step_v1',args:{p_request_id:u(1),p_actor_user_id:u(2)}}]);
 assert.deepEqual(r.result,{ok:1,fail:0});assert.equal(r.completedPrimaryOutboxSet.has(u(5)),true);assert.equal(r.warnings.length,0);});
test('queue refusal never invokes Authorise',async()=>{const r=await runCaller({queueFailure:true});
 assert.deepEqual(r.result,{ok:0,fail:1});assert.equal(r.calls.some(x=>x.name==='weekly_source_agency_change_authorise_step_v1'),false);
 assert.equal(r.completedPrimaryOutboxSet.size,0);assert.equal(r.calls[0].name,'tsfin_work_fail');});
test('late native authorise permission failure never relabels/deletes completed queue',async()=>{const r=await runCaller({nativeFailure:true});
 assert.deepEqual(r.result,{ok:1,fail:0});assert.equal(r.calls.some(x=>x.name==='tsfin_work_fail'),false);
 assert.equal(r.completedPrimaryOutboxSet.has(u(5)),true);assert.match(JSON.stringify(r.warnings),/WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED/);
 assert.doesNotMatch(JSON.stringify(r.warnings),/PRIVATE_RAW_ERROR/);});
test('malformed late native reply retains real queue outcome and finite refusal',async()=>{const r=await runCaller({nativeReply:{}});
 assert.deepEqual(r.result,{ok:1,fail:0});assert.equal(r.calls.some(x=>x.name==='tsfin_work_fail'),false);
 assert.match(JSON.stringify(r.warnings),/NATIVE_AUTHORISE_RESULT_UNCONFIRMED/);});
test('native causal hold is retained as a separate phase, not worker failure or full success',async()=>{
 const r=await runCaller({nativeReply:held()});assert.deepEqual(r.result,{ok:1,fail:0});assert.match(JSON.stringify(r.warnings),/AGENCY_INVOICE_TSFIN_NOT_READY/);});

test('full kernel reverse restores exact native body; unchanged Source scope still controls original caller',()=>{
 const r=JSON.parse(read('NATIVE_QUERY_FACTS_KERNEL_FULL_REVERSE.json'));let body=r.kernel_definition;
 for(const h of [...r.kernel_hunks].reverse()){assert.equal(body.split(h.after).length,2);body=body.replace(h.after,()=>h.before);}assert.equal(body,r.before_definition);
 assert.equal(sha(body.trimEnd()),'ce7d19b9984d8a4e2269fd705572d351a874219dff40dc17a7d857556dbde1ec');
 assert.match(r.original_owner_wrapper,/private\.weekly_source_pay_query_scope_v1\(p_root_timesheet_id\)/);
 assert.match(body,/with.*|v_manual/);assert.match(body,/v_unavailable/);assert.match(body,/WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE/);
 assert.match(body,/weekly_source_protected_wait_receipt_v1/);
});
test('owned native scope admits only real role/CAS/physical/parent and actual under-writer XID/PID',()=>{
 const s=read('51_agency_owned_native_query_facts.sqlinc');
 for(const owner of ['weekly_source_pay_actor_allowed_v1','weekly_source_office_authority_v1','weekly_source_bulk_source_cas_v1',
  'weekly_source_resolve_root_identity_v1','weekly_source_native_query_facts_for_scope_v1'])assert.ok(s.includes(owner));
 for(const guard of ['pg_current_xact_id()','pg_backend_pid()','current_timesheet_version','requested_is_canonical','family_is_current',
  'root_family_booking_id','TIMESHEET_UNAUTHORISE'])assert.ok(s.includes(guard));
 assert.doesNotMatch(s,/set_config|insert into|update public\.|grant execute[^;]*service_role/i);
});
test('actual flags preserve native unknown/subject/response proofs, higher-source rule and event-grain identity',()=>{
 const s=read('43_agency_native_segment_flags.sqlinc');
 for(const guard of ['native_query_facts','query_state_sha256','current_comparison_revision_id','material_comparison_fingerprint',
  'CANDIDATE_RESPONDED','candidate_timesheet_revision','source_minutes<=candidate_minutes','active_protection or disputed'])assert.ok(s.includes(guard));
 assert.doesNotMatch(s,/sum\(|pay_ex_vat\s*:=|coalesce\([^;\n]*blocked[^;\n]*false/i);
 assert.match(s,/candidate_id=e\.candidate_id and p\.work_event_id=e\.id/);
});
test('unchanged native calculator/flag owner is the sole economic writer in the phase',()=>{
 const apply=read('44_agency_native_segment_flag_apply.sqlinc'),step=read('46_agency_native_authorise_step.sqlinc');
 assert.equal((apply.match(/:=public\.tsfin_update_segments_locked\(/g)||[]).length,1);
 assert.equal((step.match(/:=public\.timesheet_authorise_generic_atomic\(/g)||[]).length,1);
 assert.doesNotMatch(apply+step,/total_pay_ex_vat\s*:=|total_charge_ex_vat\s*:=|insert into private\.bpay_next_|update private\.bpay_next_/i);
 assert.ok(step.indexOf('segment_flags_apply_v1')<step.indexOf(':=public.timesheet_authorise_generic_atomic'));
 assert.match(step,/phase='AUTHORISED'/);assert.doesNotMatch(step,/set state='COMPLETED'/);
});
test('after each genuine observation native flags precede the retained full current row hash',()=>{
 const s=read('45_complete_financial_observation_with_native_flags.sqlinc');
 assert.ok(s.indexOf('segment_flags_receipt:=')<s.indexOf("'financial_row_sha256'"));
 assert.match(s,/current_financial_id=f\.id,[\s\S]*returning \* into r;[\s\S]*segment_flags_receipt:=private/);
 assert.match(read('47_complete_single_writer_current_native_flag_result.sqlinc'),/IF v_agency_write_purpose IS NOT NULL THEN\s*SELECT tf\.\* INTO STRICT inserted_row/);
});
