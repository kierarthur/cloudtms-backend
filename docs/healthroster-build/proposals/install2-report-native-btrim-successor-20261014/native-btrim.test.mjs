// UNIT_ONLY codec inputs. No database, native receipt or acceptance output.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {validateNativeFinLockObservation,nativeOrdinaryRequestSha256} from './forward/scripts/weekly-source-report-fin-native-lock-observation.mjs';
import {validateNativeFinLockObservation as oldDecoder} from '../install2-shared-runtime-forward-20261014/forward/scripts/weekly-source-report-fin-native-lock-observation.mjs';
import {sha256} from './forward/scripts/weekly-source-protected-release-qualification.mjs';
const id='00000000-0000-4000-8000-000000000001',hash='a'.repeat(64),execution={UNIT_ONLY:'not a release'},dir=path.dirname(fileURLToPath(import.meta.url));
function unitNextRecord(raw,canonical){
 const r={actor_user_id:id,source_cycle_id:id,authority_scope_kind:'CYCLE',report_scope_id:null,upload_id:id,projection_publication_id:id,
  expected_authority_scope_version:1,expected_row_manifest_hash:hash,expected_comparison_manifest_hash:hash,expected_issue_set_hash:hash,exclude_unfinalised_acknowledged:false};
 const argument=JSON.stringify({p_request:r,p_correction_session_id:null,p_prepare_only:false,p_report_vat_rounding_variant:'A'}),result=JSON.stringify({UNIT_ONLY:'not a native result'});
 const authorization={purpose:'SOURCE_FIXTURE_SETUP',authorization_sha256:hash,source_cycle_id:id,source_group_id:id,journey_actor_user_id:id,fixture_manifest:{client_ids:[id]}},
  command={schema_version:'WEEKLY_SOURCE_OFF_RULE_COMMAND_V1',command_id:id,actor_user_id:id,source_cycle_id:id,backend_pid:'123',transaction_id:'999',
   native_operation:'weekly_source_finalise_atomic_v1',authorization,operational_ready:false,native_parent_scope:{schema_version:'WEEKLY_SOURCE_OFF_RULE_NATIVE_PARENT_SCOPE_V1',source_cycle_id:id,ambiguous:false},native_request_sha256:nativeOrdinaryRequestSha256(r)};
 const commandBase=JSON.stringify(command);command.command_sha256=sha256('WEEKLY_SOURCE_OFF_RULE_COMMAND_V1\x1f'+commandBase);
 const facts=JSON.stringify({client_ids:[id]}),frame={schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1',execution,command_id:id,command_sha256:command.command_sha256,
  authorization_sha256:hash,native_operation:command.native_operation,actor_user_id:id,source_cycle_id:id,upload_id:id,projection_publication_id:id,generation:1,authority_scope_kind:'CYCLE',prepare_only:false,native_argument_bytes:argument,
  backend_pid:'123',transaction_id:'999',active_owner:'NEXT',original_root_ids:null,rechecked_root_ids:null,root_set_setting:'',operation_id:id,operation_setting:id,
  operation:{id,transaction_id:'999',upload_id:id,projection_publication_id:id,generation:1,old_revision_id:null,source_group_id:id,client_id:id,coverage_start:'2026-09-21',coverage_end:'2026-09-27'},
  operation_members:[{operation_id:id,root_timesheet_id:id,raw_booking_id:raw,canonical_booking_id:canonical}],lock_result:null,
  source_lock_discovery:{UNIT_ONLY:'not native discovery'},source_lock_observed:{UNIT_ONLY:'not native discovery'},window_lock_discovery:null,window_lock_observed:null,
  source_cas:{schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:id,client_ids:[id],source_cas_sha256:sha256('WEEKLY_SOURCE_BULK_SOURCE_CAS_V1\x1f'+facts)},source_facts_bytes:facts,operational_ready:false};
 const frameBase=JSON.stringify(frame);frame.frame_sha256=sha256('WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1\x1f'+frameBase);
 const before=JSON.stringify(command),record={id,object_type:'weekly_source_off_rule_native_fin',object_id_text:id,actor_user_id:id,event_sequence:'2',event_sequence_is_authoritative:true,before_bytes:before,after_bytes:JSON.stringify(frame),
  frame_base_bytes:frameBase,command_base_bytes:commandBase,completed_before_bytes:before,completed_sequence:'3',completed_bytes:JSON.stringify({command_id:id,command_sha256:command.command_sha256,operational_ready:false,native_result_sha256:sha256('WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1\x1f'+result)})};
 return {record,context:{execution,actor_user_id:id,backend_pid:'123',transaction_id:'999',native_result_bytes:result,native_argument_bytes:argument}};
}
for(const [name,raw,canonical]of [['ASCII spaces',' abc ','abc'],['tabs','\tabc\t','\tabc\t'],['NBSP','\u00a0abc\u00a0','\u00a0abc\u00a0'],
 ['mixed',' \tabc\t ','\tabc\t'],['final newline','abc \n','abc \n'],['BOM',' \ufeffabc\ufeff ','\ufeffabc\ufeff'],['tab-only','\t','\t']])
 test('UNIT_ONLY: native btrim preserves exact '+name+' booking bytes',()=>{const {record,context}=unitNextRecord(raw,canonical),decoded=validateNativeFinLockObservation(record,context);
  assert.equal(decoded.frame.operation_members[0].raw_booking_id,raw);assert.equal(decoded.frame.operation_members[0].canonical_booking_id,canonical);});
for(const raw of ['\tabc\t','\u00a0abc\u00a0'])test('UNIT_ONLY: prior JS trim rejects a natively permitted member',()=>{
 const {record,context}=unitNextRecord(raw,raw);assert.throws(()=>oldDecoder(record,context),/REPORT_NATIVE_LOCK_MEMBER_INVALID/);validateNativeFinLockObservation(record,context);});
test('UNIT_ONLY: replacing native tab canonical with JS-normalized text remains refused',()=>{
 const {record,context}=unitNextRecord('\tabc\t','abc');assert.throws(()=>validateNativeFinLockObservation(record,context),/REPORT_NATIVE_LOCK_MEMBER_INVALID/);});
test('UNIT_ONLY: only ASCII spaces produce forbidden empty canonical booking',()=>{
 const {record,context}=unitNextRecord('   ','');assert.throws(()=>validateNativeFinLockObservation(record,context),/REPORT_NATIVE_LOCK_MEMBER_INVALID/);});
test('UNIT_ONLY: full XID scope still refuses after the narrow btrim change',()=>{
 const {record,context}=unitNextRecord('\tabc\t','\tabc\t');context.transaction_id='1000';assert.throws(()=>validateNativeFinLockObservation(record,context),/REPORT_NATIVE_LOCK_SCOPE_UNPROVED/);});
test('UNIT_ONLY: the two complete hunks reverse to exact frozen35 decoder',()=>{
 const proof=JSON.parse(fs.readFileSync(path.join(dir,'NATIVE_BTRIM_COMPLETE_REVERSE.json'))),before=fs.readFileSync(path.resolve(dir,'../../../..',proof.input),'utf8');let after=fs.readFileSync(path.join(dir,proof.output),'utf8');
 for(const h of [...proof.hunks].reverse()){assert.equal(after.split(h.new).length,2);after=after.replace(h.new,()=>h.old);}assert.equal(after,before);
});
