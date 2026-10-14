import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';import {execFileSync} from 'node:child_process';
import {unitRawTrace} from '../report-ab-protected-dispatch/unit-raw-collector-fixture.mjs';
import {sha256,domainHash} from '../report-ab-protected-dispatch/weekly-source-protected-release-qualification.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),mount=path.join(dir,'.merge-inputs/test-mount/scripts');
const {deriveReportNativeReceipt}=await import(pathToFileURL(path.join(mount,'weekly-source-report-fin-native-collector.mjs')).href);
const {deriveReportCaseGrain,validateNextOrigin,validateNegativeProvenance,reportNativeI1ProbeSql,reportNativeMapperQueries,validateNativeOrdinaryFinJourneys}=await import(pathToFileURL(path.join(mount,'weekly-source-report-fin-native-mapper.mjs')).href);
const {nativeOrdinaryRequestSha256,validateNativeFinLockObservation,reportNativeFinLockObservationSql}=await import(pathToFileURL(path.join(mount,'weekly-source-report-fin-native-lock-observation.mjs')).href);
const id='10000000-0000-4000-8000-000000000001',hash='a'.repeat(64),execution={release_id:'UNIT_ONLY_NOT_NATIVE',source_commit:'a'.repeat(40),contract_sha256:hash,
 installation_id:id,environment:'TEST',database:'cloudtms_test_clone',managed_manifest_sha256:hash,installed_catalogue_jsonb_sha256:hash};
test('UNIT_ONLY: null literal compiled native seal keeps every otherwise complete codec trace non-recordable',()=>{
 const result=deriveReportNativeReceipt(JSON.stringify(unitRawTrace(execution)));assert.equal(result.status,'WAITING_NATIVE_GATES');assert.equal(result.receipt,null);
 assert.ok(result.waiting.includes('native_mapper_compiled_contract_mount'));
});
test('UNIT_ONLY: critic arbitrary owner/body/request and rehashed {} equality counterexample cannot mint a receipt',()=>{
 const trace=unitRawTrace(execution);for(const gate of Object.values(trace.native_gate_facts)){
  gate.owner_signature='private.unreviewed_arbitrary_owner_v1()';gate.owner_body_sha256='b'.repeat(64);
  const input=JSON.parse(gate.request_bytes);input.native_owner_request={};gate.request_bytes=JSON.stringify(input);
  const result=JSON.parse(gate.result_bytes);result.native_request_sha256=sha256(gate.request_bytes);
  for(const o of result.observations)o.expected_bytes=o.observed_bytes='{}';
  result.native_result_sha256=domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_GATE_OBSERVATIONS_V1',result.observations);gate.result_bytes=JSON.stringify(result);
 }
 const result=deriveReportNativeReceipt(JSON.stringify(trace));assert.equal(result.status,'WAITING_NATIVE_GATES');assert.equal(result.receipt,null);
});
for(const caseId of ['BOUND_NEGATIVE','STANDALONE_NEGATIVE'])test('UNIT_ONLY: positive100 relabelled '+caseId+' independently refuses before unmounted gates',()=>{
 assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(unitRawTrace(execution,{caseId}))),/NEGATIVE_GRAIN_UNPROVED/);
});
for(const caseId of ['D_NEGATIVE','D_POSITIVE'])test('UNIT_ONLY: actual zero native rounding cannot be labelled '+caseId,()=>{
 assert.throws(()=>deriveReportNativeReceipt(JSON.stringify(unitRawTrace(execution,{caseId}))),/ROUNDING_SIGN_MISMATCH/);
});
test('UNIT_ONLY: standalone negative requires genuine signed row, all four actual NULL identities and no binding/prior',()=>{
 const trace=unitRawTrace(execution),basis=JSON.parse(trace.basis_bytes),financial=JSON.parse(trace.financial_bytes),m=financial.movements[0];
 basis.rows[0].source_shift_charge_pence=-100;basis.rows[0].source_vat_pence=-20;basis.rows[0].source_gross_pence=-120;
 m.movement_role='STANDALONE_NEGATIVE';m.prior_movement_id=null;for(const k of ['candidate_id','contract_id','work_event_id','invoice_timesheet_id'])m[k]=null;
 for(const k of ['prior_report_movement_id','report_line_binding_id','bound_row_resolution_id','bound_export_projection_publication_id'])m.source_facts_json[k]=null;
 assert.equal(deriveReportCaseGrain('STANDALONE_NEGATIVE',basis,financial).negative_row_ids[0],id);
 m.candidate_id=id;assert.throws(()=>deriveReportCaseGrain('STANDALONE_NEGATIVE',basis,financial),/STANDALONE_NEGATIVE_GRAIN/);
});
test('UNIT_ONLY: genuine negative cannot retain POSITIVE role or hide signed economics in a caller annotation',()=>{
 const trace=unitRawTrace(execution),basis=JSON.parse(trace.basis_bytes),financial=JSON.parse(trace.financial_bytes);basis.rows[0].source_shift_charge_pence=-100;
 assert.throws(()=>deriveReportCaseGrain('D_ZERO',basis,financial),/NEGATIVE_ROLE_INVALID/);
});
test('UNIT_ONLY: scoped native query family retains actual full native PAY/prior/ordinary/effect witnesses without generic equality',()=>{
 const t=unitRawTrace(execution),b=JSON.parse(t.basis_bytes),queries=reportNativeMapperQueries(t.native_request,b.authorization);
 assert.match(queries.pay_authorities,/weekly_source_self_bill_report_pay_authority_v1/);assert.match(queries.priors,/prior_movement/);
 assert.match(queries.effects,/mail_outbox/);assert.match(queries.ordinary_rows,/weekly_source_effective_inventory_v1/);
 assert.throws(()=>reportNativeI1ProbeSql(id,[], 'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION'),/I1_PROBE_REQUEST_INVALID/);
 assert.throws(()=>reportNativeI1ProbeSql(id,[id], 'FINALISED'),/I1_PROBE_REQUEST_INVALID/);
 assert.match(reportNativeI1ProbeSql(id,[id], 'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION'),/weekly_source_lock_and_resolve_families_v1/);
});
function unitLockRecord(){
 const r={...unitRawTrace(execution).native_request,authority_scope_kind:'CYCLE',report_scope_id:null},args={p_request:r,p_correction_session_id:null,p_prepare_only:false,p_report_vat_rounding_variant:'A'},
  argBytes=JSON.stringify(args),resultBytes=JSON.stringify({ok:true,status:'FINALISED',source_cycle_id:id,final_revision_id:id});
 const authorization={purpose:'SOURCE_FIXTURE_SETUP',authorization_sha256:hash,source_cycle_id:id,source_group_id:id,journey_actor_user_id:id,fixture_manifest:{client_ids:[id]}},
  command={schema_version:'WEEKLY_SOURCE_OFF_RULE_COMMAND_V1',command_id:id,actor_user_id:id,source_cycle_id:id,backend_pid:'123',transaction_id:'999',
   native_operation:'weekly_source_finalise_atomic_v1',authorization,operational_ready:false,native_parent_scope:{schema_version:'WEEKLY_SOURCE_OFF_RULE_NATIVE_PARENT_SCOPE_V1',source_cycle_id:id,ambiguous:false},
   native_request_sha256:nativeOrdinaryRequestSha256(r)};
 const commandBase=JSON.stringify(command);command.command_sha256=sha256('WEEKLY_SOURCE_OFF_RULE_COMMAND_V1\x1f'+commandBase);
 const facts=JSON.stringify({client_ids:[id]}),frame={schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1',execution,command_id:id,command_sha256:command.command_sha256,
  authorization_sha256:hash,native_operation:command.native_operation,actor_user_id:id,source_cycle_id:id,upload_id:id,projection_publication_id:id,generation:1,authority_scope_kind:'CYCLE',prepare_only:false,
  native_argument_bytes:argBytes,backend_pid:'123',transaction_id:'999',active_owner:'LEGACY',original_root_ids:null,rechecked_root_ids:null,root_set_setting:'EMPTY_LOCKED_SET',
  operation_id:null,operation_setting:'',operation:null,operation_members:[],lock_result:null,source_lock_discovery:{UNIT_ONLY:'native-shaped'},source_lock_observed:{UNIT_ONLY:'native-shaped'},
  window_lock_discovery:null,window_lock_observed:null,source_cas:{schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:id,client_ids:[id],source_cas_sha256:sha256('WEEKLY_SOURCE_BULK_SOURCE_CAS_V1\x1f'+facts)},
  source_facts_bytes:facts,operational_ready:false};
 const frameBase=JSON.stringify(frame);frame.frame_sha256=sha256('WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1\x1f'+frameBase);
 const before=JSON.stringify(command),record={id,object_type:'weekly_source_off_rule_native_fin',object_id_text:id,actor_user_id:id,event_sequence:'2',event_sequence_is_authoritative:true,
  before_bytes:before,after_bytes:JSON.stringify(frame),frame_base_bytes:frameBase,command_base_bytes:commandBase,completed_before_bytes:before,completed_sequence:'3',
  completed_bytes:JSON.stringify({command_id:id,command_sha256:command.command_sha256,operational_ready:false,native_result_sha256:sha256('WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1\x1f'+resultBytes)})};
 return {record,context:{execution,actor_user_id:id,backend_pid:'123',transaction_id:'999',native_result_bytes:resultBytes,native_argument_bytes:argBytes}};
}
test('UNIT_ONLY: native under-lock decoder preserves actual NULL roots and literal EMPTY_LOCKED_SET without fabricating arrays',()=>{
 const {record,context}=unitLockRecord();assert.equal(validateNativeFinLockObservation(record,context).frame.original_root_ids,null);
 assert.match(reportNativeFinLockObservationSql(id,id),/pg_current_xact_id_if_assigned/);
});
for(const [label,change]of [['wrong XID',f=>f.transaction_id='1000'],['invented empty roots',f=>f.original_root_ids=[]],['fake NEXT context',f=>f.active_owner='NEXT'],
 ['Report substitution',f=>f.authority_scope_kind='SELF_BILL_REPORT_SCOPE'],['source recheck drift',f=>f.source_lock_observed={UNIT_ONLY:'changed'}]])
 test('UNIT_ONLY: rehashed actual lock frame refuses '+label,()=>{
  const {record,context}=unitLockRecord(),frame=JSON.parse(record.after_bytes);change(frame);const {frame_sha256:ignored,...base}=frame;
  record.frame_base_bytes=JSON.stringify(base);frame.frame_sha256=sha256('WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1\x1f'+record.frame_base_bytes);record.after_bytes=JSON.stringify(frame);
  assert.throws(()=>validateNativeFinLockObservation(record,context));
 });
test('UNIT_ONLY: complete FIN4 successor reverses to exact current committed body and leaves Report early leaf/economic/lock body intact',()=>{
 const proof=JSON.parse(fs.readFileSync(path.join(dir,'FIN4_NATIVE_LOCK_OBSERVATION_REVERSE.json'))),file=path.join(dir,proof.output_file);let reverse=fs.readFileSync(file,'utf8');
 for(const h of [...proof.hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}
 const own=path.resolve(dir,'../../../..'),original=execFileSync('git',['show',proof.input_commit+':'+proof.input_file],{cwd:own,encoding:'utf8',maxBuffer:16*1024*1024});assert.equal(reverse,original);
 const observed=fs.readFileSync(file,'utf8');assert.ok(observed.indexOf('weekly_source_self_bill_report_finalise_v1')<observed.indexOf('WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_OBSERVED'));
 assert.equal((observed.match(/create or replace function/g)??[]).length,(original.match(/create or replace function/g)??[]).length);
 assert.equal((observed.match(/set_config\(/g)??[]).length,(original.match(/set_config\(/g)??[]).length);
});
test('UNIT_ONLY: ordinary journey {} pairs and negative provenance empty selector cannot substitute for native owner rows',()=>{
 assert.throws(()=>validateNativeOrdinaryFinJourneys([{},{}],{}));
 assert.throws(()=>validateNegativeProvenance([{}],{basis:{rows:[{id,source_shift_charge_pence:-1}]},financial:{movements:[]}}));
 assert.throws(()=>validateNextOrigin({ok:true,code:'OK'}));
});
