import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createOffRuleNativeDependencies,sourceOperations,automaticOperations} from './off-rule-native-proxy.mjs';
import {buildOffRuleTestAuthorization,assertManagedOffRuleTestEvidence} from './off-rule-setup-evidence.mjs';
import {domainHash} from '../protected-source-capability/release-qualification.mjs';
import {buildMeasuredNativePartial} from './measured-partial-evidence.mjs';
import {createHash} from 'node:crypto';
const out=path.dirname(fileURLToPath(import.meta.url)),id=n=>`91301300-0000-4000-8000-${String(n).padStart(12,'0')}`,h='a'.repeat(64);
const scope={actorUserId:id(1),agencyId:id(2),environment:'TEST',sourceCycleId:id(3),purpose:'SOURCE_FIXTURE_SETUP'};
// UNIT VECTORS ONLY: no one of these synthetic strings is an acceptance input.
function vectors(){const context={environment:'TEST',expectedDatabase:'cloudtms_test_clone',mode:'UPGRADE',releaseId:'UNIT_VECTOR_NOT_NATIVE_EVIDENCE',gitCommit:'b'.repeat(40),expectedHash:h};
 const controls={schema_version:'WEEKLY_SOURCE_OFF_RULE_TEST_CONTROLS_V1',release_id:context.releaseId,source_commit:context.gitCommit,contract_sha256:h,
  banking_install_receipt_sha256:h,security_receipt_sha256:h,source_effects_review_sha256:h,banking_first_install_complete:true,effective_acl_pass:true,browser_denied:true,source_effects_no_external_calls:true};
 const input={authorization_id:id(4),scope_id:id(5),agency_id:id(2),source_group_id:id(6),source_cycle_id:id(3),setup_actor_user_id:id(7),journey_actor_user_id:id(1),
  purpose:'SOURCE_FIXTURE_SETUP',week_ending:'2026-10-18',expires_at_utc:'2026-10-23T12:00:00Z',fixture_namespace:'WSQ_TEST_',
  fixture_manifest:{group_code:'WSQ_TEST_INTERFACE',client_ids:[id(8)],contract_ids:[id(9)],candidate_ids:[id(10)],
   client_refs:[{client_id:id(8),cli_ref:'CLI-91301301'}],candidate_refs:[{candidate_id:id(10),tms_ref:'CCR-91301301'}]},
  assignment_id:id(11),assignment_version:'1',assignment_sha256:h,rule_set_version_id:id(12),rule_values_sha256:h,original_source_cas:null,
  allowed_operations:[...sourceOperations].sort(),banking_install_receipt_sha256:h,security_receipt_sha256:h,source_effects_review_sha256:h};
 return {context,controls,input};}
test('managed OFF constructor and revalidator have exactly the same mandatory closure',()=>{
 const {context,controls,input}=vectors();const q=buildOffRuleTestAuthorization(context,input,controls);
 const evidence={weekly_source_off_rule_test_controls:controls,weekly_source_protected_test_setup_authorizations:[q]};
 assert.doesNotThrow(()=>assertManagedOffRuleTestEvidence(context,evidence));
 for(const key of ['banking_first_install_complete','effective_acl_pass','browser_denied','source_effects_no_external_calls']){
  const bad=structuredClone(evidence);bad.weekly_source_off_rule_test_controls[key]=false;
  assert.throws(()=>assertManagedOffRuleTestEvidence(context,bad),/CONTROLS_REQUIRED/);}
 for(const key of ['rule_set_version_id','assignment_version','rule_values_sha256','allowed_operations','fixture_manifest','original_source_cas']){
  const bad=structuredClone(evidence);bad.weekly_source_protected_test_setup_authorizations[0][key]=key==='original_source_cas'?{}:null;
  const row=bad.weekly_source_protected_test_setup_authorizations[0];const {authorization_sha256,...body}=row;
  row.authorization_sha256=domainHash(row.schema_version,body);assert.throws(()=>assertManagedOffRuleTestEvidence(context,bad));}
});
test('an Automatic scope needs exactly one original Client and literal Source CAS; no S18b override accepted',()=>{
 const {context,controls,input}=vectors();input.purpose='AUTOMATIC_JOURNEY';input.allowed_operations=[...automaticOperations].sort();
 assert.throws(()=>buildOffRuleTestAuthorization(context,input,controls),/SOURCE_CAS_INVALID/);
 input.original_source_cas={schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:input.source_cycle_id,client_ids:input.fixture_manifest.client_ids,source_cas_sha256:h};
 assert.doesNotThrow(()=>buildOffRuleTestAuthorization(context,input,controls));
 input.equal_protected_hours='CLEAR_AUTOMATICALLY';assert.throws(()=>buildOffRuleTestAuthorization(context,input,controls),/INPUT_INVALID/);
});
test('ordinary and LIVE dependencies are returned unchanged; a claimed malformed admission refuses',async()=>{
 let calls=0;const dependencies={dataRpc:async()=>{calls++;return {contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:false,operational_ready:false};}};
 assert.equal(await createOffRuleNativeDependencies({...scope,dependencies}),dependencies);assert.equal(calls,1);
 assert.equal(await createOffRuleNativeDependencies({...scope,environment:'LIVE',dependencies}),dependencies);assert.equal(calls,1);
 dependencies.dataRpc=async()=>({contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:true,operational_ready:true});
 await assert.rejects(createOffRuleNativeDependencies({...scope,dependencies}),/ADMISSION_INVALID/);
});
test('qualified commands preserve the full actual native request and trusted server actor; no browser selector is passed',async()=>{
 const calls=[];const dependencies={calculator:{unchanged:true},dataRpc:async(name,args)=>{
  calls.push({name,args});if(calls.length===1)return {contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:true,operational_ready:false,
   purpose:scope.purpose,source_cycle_id:scope.sourceCycleId,actor_user_id:scope.actorUserId,allowed_operations:[...sourceOperations]};return {native:true};}};
 const wrapped=await createOffRuleNativeDependencies({...scope,dependencies});const req={actor_user_id:scope.actorUserId,source_cycle_id:scope.sourceCycleId,rows:[{literal:'whole native payload'}]};
 assert.equal(wrapped.calculator,dependencies.calculator);assert.deepEqual(await wrapped.dataRpc('weekly_source_upload_stage_rows_atomic_v1',{p_request:req}),{native:true});
 assert.equal(calls[1].args.p_request.native_request,req);assert.equal(calls[1].args.p_request.actor_user_id,scope.actorUserId);
 assert.deepEqual(Object.keys(calls[1].args.p_request).sort(),['actor_user_id','agency_id','environment','qualification_action','source_cycle_id','native_operation','native_request'].sort());
 await assert.rejects(wrapped.dataRpc('weekly_source_finalise_atomic_v1',{p_request:req,qualification_enabled:true}),/NATIVE_PAYLOAD_INVALID/);
});
test('the native two-parameter reader, Source CAS projection, revision and calendar owners keep the boundary closed',()=>{
 const sql=file=>fs.readFileSync(path.join(out,file),'utf8');
 assert.match(sql('02_effective_rule_owner.sqlinc'),/if auth is null then return null/);
 assert.match(sql('03_cas_operational_projection.sqlinc'),/weekly_source_operational_effective_rule_set_v1\(client,c.id\)/);
 assert.doesNotMatch(sql('03_cas_operational_projection.sqlinc'),/weekly_source_rule_qualification_context_v1/);
 assert.match(sql('04_final_revision_rule_guard.sqlinc'),/if tg_op='UPDATE'/);
 assert.match(sql('04_final_revision_rule_guard.sqlinc'),/WEEKLY_SOURCE_FINAL_RULE_VERSION_IMMUTABLE/);
 assert.match(sql('05_coverage_floor_owner.sqlinc'),/WEEKLY_SOURCE_OFF_RULE_TEST_OPENED/);
 assert.match(sql('OFF_RULE_AUTHORITY.sqlinc'),/a.effective_from_week_ending<today or c.finalisation_week_ending<>a.effective_from_week_ending/);
 assert.doesNotMatch(sql('OFF_RULE_AUTHORITY.sqlinc'),/set_config\(|p_now|feature_enabled\s*=\s*true/i);
 assert.match(sql('OFF_RULE_COMMAND.sqlinc'),/case op/);assert.doesNotMatch(sql('OFF_RULE_COMMAND.sqlinc'),/execute\s+(?:format|op)/i);
 const proofs=JSON.parse(sql('OWNER_SOURCE_REVERSE_PROOFS.json'));assert.equal(proofs.edits.length,6);
 assert.ok(proofs.edits.every(row=>row.reverse_hunk_matches_entire_input_definition===true));
});
test('a real-shaped non-NEXT partial does not manufacture NEXT or an operational certificate',()=>{
 const context={release_id:'UNIT_VECTOR_NOT_NATIVE_EVIDENCE',git_commit:'b'.repeat(40),contract_sha256:h,agency_id:id(2),environment:'TEST',
  database:'cloudtms_test_clone',source_group_id:id(6),effective_from_week_ending:'2026-10-18',effective_to_week_ending:null,capabilities:['BULK_SIGNED']};
 const row={schema_version:'WEEKLY_SOURCE_NATIVE_JOURNEY_RECEIPT_V1',gate:'OWN_STAGE_CAS',release_id:context.release_id,source_commit:context.git_commit,
  contract_sha256:h,agency_id:context.agency_id,environment:'TEST',database:'cloudtms_test_clone',source_group_id:context.source_group_id,
  effective_from_week_ending:context.effective_from_week_ending,effective_to_week_ending:null,execution:'AGENCY_TEST_NATIVE',
  native_execution_receipt:{accepted:true,database:'cloudtms_test_clone',rolledBack:true,connectedFresh:true,tool_receipt_sha256:h,native_journey_id:'PROTECTED_CAPABILITY_OWN_STAGE_CAS'},
  assertions:{own_stage_excluded:true,foreign_stage_not_excluded:true,completed_client_retained:true}};
 // Use the literal original assertion census, not guessed assertion names.
 return import('../protected-source-capability/release-qualification.mjs').then(({gates})=>{
  row.assertions=Object.fromEntries(gates.OWN_STAGE_CAS.map(key=>[key,true]));
  const bytes=Buffer.from(JSON.stringify(row)),digest=createHash('sha256').update(bytes).digest('hex');
  const q=buildMeasuredNativePartial(context,[{gate:'OWN_STAGE_CAS',receipt_id:'UNIT_VECTOR_NOT_NATIVE_EVIDENCE',receipt_bytes:bytes,expected_receipt_sha256:digest}]);
  assert.equal(q.operational_ready,false);assert.equal(q.phase,'MEASURED_PARTIAL_NON_OPERATIONAL');assert.ok(q.held_gates.includes('NEXT_PAY'));
  assert.equal(q.gate_receipts.length,1);assert.doesNotMatch(q.schema_version,/OPERATIONAL_QUALIFICATION/);
 });
});
test('same installed evidence record preserves every release identity field and refuses replacing evidence',()=>{
 const sql=fs.readFileSync(path.join(out,'POST_VERIFIED_EVIDENCE_RECORD.sqlinc'),'utf8');
 assert.match(sql,/update private.cloudtms_database_releases as release_row set evidence_json=next_evidence/);
 assert.doesNotMatch(sql,/set status=|set git_commit=|insert into private.cloudtms_(?:migration|repeatable)_ledger|create table/i);
 assert.match(sql,/identity_after is distinct from identity_before/);assert.match(sql,/POST_VERIFIED_REPLACEMENT_REFUSED/);
 assert.match(sql,/POST_VERIFIED_AMBIGUOUS_SCOPE_REFUSED/);assert.match(sql,/lock_row.database=/);
 assert.match(sql,/cloudtms_database_release_admission_v1/);assert.match(sql,/lock_row.classid=20261013::oid/);
 const forward=JSON.parse(fs.readFileSync(path.join(out,'SAME_INSTALLED_CERTIFICATE_REVERSE.json')));
 assert.equal(forward.proofs.length,2);assert.ok(forward.proofs.every(p=>p.reverse_hunk_matches_entire_input_definition));
 const cert=fs.readFileSync(path.join(out,'SAME_INSTALLED_OPERATIONAL_CERTIFICATE.sqlinc'),'utf8');
 assert.doesNotMatch(cert,/executed.release_id=r.release_id or/);assert.match(cert,/executed.status<>'VERIFIED'/);
 assert.match(cert,/ALL_PRE_SWITCH_GATES_QUALIFIED/);assert.match(cert,/PHYSICAL_TESTFLIGHT_MYTMS_PHONE/);
});
