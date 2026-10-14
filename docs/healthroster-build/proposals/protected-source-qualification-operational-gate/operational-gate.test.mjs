import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {parseGitTree,assertTreeEquivalence,sourceInventory,proveCommittedSources,releaseAuthority,releaseAuthorityHash} from './source-equivalence.mjs';
import {buildOperationalQualification,requireOperationalCertificate,activationReceiptReady,remoteGates,assertManagedOperationalEvidence} from './operational-evidence.mjs';
import {gates,domainHash} from '../protected-source-capability/release-qualification.mjs';
const root=fileURLToPath(new URL('../../../../',import.meta.url)),read=file=>fs.readFileSync(new URL(file,import.meta.url),'utf8');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex'),id='00110011-0011-4011-8011-001100110011',hash='1'.repeat(64);
// Pure synthetic protocol vectors, NEVER acceptance evidence. No certificate or
// fake PASS is serialized to disk, committed as a receipt, or installed.
const context={release_id:'PROTOCOL_ONLY_R2',git_commit:'2'.repeat(40),contract_sha256:hash,agency_id:id,
 environment:'TEST',database:'cloudtms_test_clone',source_group_id:id,effective_from_week_ending:'2026-10-18',effective_to_week_ending:null,capabilities:['BULK_SIGNED']};
const nativeContext={...context,release_id:'PROTOCOL_ONLY_R1',git_commit:'1'.repeat(40)};
function vectors(){
 const nativeRows=Object.keys(gates).filter(gate=>gate!=='AUTOMATIC_COMPONENT_DELTA').map(gate=>({
  schema_version:'WEEKLY_SOURCE_NATIVE_JOURNEY_RECEIPT_V1',execution:'AGENCY_TEST_NATIVE',gate,release_id:nativeContext.release_id,
  source_commit:nativeContext.git_commit,contract_sha256:hash,agency_id:id,environment:'TEST',database:'cloudtms_test_clone',source_group_id:id,
  effective_from_week_ending:'2026-10-18',effective_to_week_ending:null,
  native_execution_receipt:{accepted:true,database:'cloudtms_test_clone',rolledBack:true,connectedFresh:true,tool_receipt_sha256:hash,
   native_journey_id:'PROTECTED_CAPABILITY_'+gate},assertions:Object.fromEntries(gates[gate].map(key=>[key,true]))}));
 const closure={scope_id:id,scope_sha256:hash,setup_authorization_sha256:hash,original_census_sha256:hash,actor_user_id:id,
  source_cycle_id:id,week_ending:'2026-10-18',execution_release_id:nativeContext.release_id,execution_source_commit:nativeContext.git_commit,
  execution_contract_sha256:hash,closure_audit_id:id,closure_audit_sha256:hash,state:'CLOSED',closed_at_utc:'2026-10-22T12:00:00Z',
  all_assignments_disabled:true,all_operational_producers_false:true};
 const remoteRows=Object.entries(remoteGates).map(([gate,spec])=>({schema_version:'WEEKLY_SOURCE_PROTECTED_REMOTE_GATE_RECEIPT_V1',gate,origin:spec.origin,
  execution_release_id:nativeContext.release_id,execution_source_commit:nativeContext.git_commit,execution_contract_sha256:hash,
  environment:'TEST',database:'cloudtms_test_clone',agency_id:id,source_group_id:id,source_cycle_id:id,actor_user_id:id,week_ending:'2026-10-18',
  scope_id:id,scope_sha256:hash,original_census_sha256:hash,server_request_ids:['PROTOCOL_ONLY_REQUEST'],server_trace_sha256:hash,
  native_result_receipts_sha256:hash,capture_sha256:hash,deployments:{worker_script:'test-cloudtms-backend',worker_version_id:id,
   worker_source_commit:nativeContext.git_commit,office_source_commit:'3'.repeat(40),office_bundle_sha256:hash,mytms_source_commit:'4'.repeat(40),
   mytms_build_id:'PROTOCOL_ONLY_BUILD',mytms_bundle_identifier:'net.cloudtms.mytms.test',mytms_build_artifact_sha256:hash},
  assertions:Object.fromEntries(spec.assertions.map(key=>[key,true]))}));
 const pack=rows=>rows.map(row=>{const bytes=Buffer.from(JSON.stringify(row));return {gate:row.gate,receipt_id:'PROTOCOL_ONLY/'+row.gate,
  receipt_bytes:bytes,expected_receipt_sha256:sha(bytes)};});
 const sourceBody={schema_version:'WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2',execution_source_commit:nativeContext.git_commit,
  installation_source_commit:context.git_commit,inventory_schema:'WEEKLY_SOURCE_NATIVE_RELEASE_TOOLCHAIN_VERIFIER_INVENTORY_V2',
  files:sourceInventory.required.map(file=>({path:file,git_mode:'100644',git_type:'blob',execution_blob_oid:'a'.repeat(40),installation_blob_oid:'a'.repeat(40),
   execution_sha256:hash,installation_sha256:hash,comparison:file===sourceInventory.manifest?'RELEASE_AUTHORITY_EXCEPT_CLOSED_RECEIPT_DESCRIPTORS':'EXACT_GIT_OBJECT',comparison_sha256:hash})),receipt_bindings:[]};
 const source={...sourceBody,source_equivalence_sha256:domainHash(sourceBody.schema_version,sourceBody)};
 return {nativeRows,remoteRows,closure,source,build(){return buildOperationalQualification(context,nativeContext,pack(nativeRows),pack(remoteRows),closure,source);}};
}
test('V2 pending prerequisite and native-only V1 refuse at activation/retained receipt/readiness protocol boundary',()=>{
 for(const schema of ['WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2','WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1']){
  const q={schema_version:schema,phase:'PREREQUISITES_QUALIFIED_OPERATIONAL_REPLAY_PENDING',capabilities:['BULK_SIGNED']};
  assert.throws(()=>requireOperationalCertificate(q),/OPERATIONAL_GATES_REQUIRED/);
  assert.equal(activationReceiptReady(q,{feature_enabled:true,protected_capability_qualification:q}),false);
 }
});
test('full protocol preserves R1 execution and all original nine gates plus three distinct remote origins',()=>{
 const q=vectors().build();assert.equal(q.native_execution_qualification.release_id,'PROTOCOL_ONLY_R1');assert.equal(q.release_id,'PROTOCOL_ONLY_R2');
 assert.equal(q.native_execution_qualification.gate_receipts.length,9);assert.equal(q.remote_gate_receipts.length,3);
 assert.equal(activationReceiptReady(q,{feature_enabled:false,protected_capability_qualification:q}),false);
});
test('native callbacks, handler bridges and SQL payload tests cannot become deployed HTTP/UI/phone evidence',()=>{
 for(const gate of Object.keys(remoteGates)){
  const v=vectors();v.remoteRows.find(row=>row.gate===gate).origin='SAME_SESSION_NATIVE_HANDLER_BRIDGE';
  assert.throws(()=>v.build(),/REMOTE_SCOPE_INVALID/);
 }
});
test('each omitted/failed actual app gate or missing immutable closure refuses',()=>{
 const missing=vectors();missing.remoteRows.pop();assert.throws(()=>missing.build(),/REMOTE_GATE_CENSUS/);
 for(const gate of Object.keys(remoteGates)){
  const v=vectors();v.remoteRows.find(row=>row.gate===gate).assertions[remoteGates[gate].assertions[0]]=false;
  assert.throws(()=>v.build(),/REMOTE_ASSERTION_FAILED/);
 }
 for(const [key,value]of [['state','OPEN'],['all_assignments_disabled',false],['all_operational_producers_false',false]]){
  const v=vectors();v.closure[key]=value;assert.throws(()=>v.build(),/SETUP_CLOSURE_INVALID/);
 }
});
test('foreign actor/scope/cycle/execution and mixed deployed builds refuse without replacing execution identity',()=>{
 for(const field of ['actor_user_id','scope_id','source_cycle_id','execution_release_id']){
  const v=vectors();v.remoteRows[0][field]='foreign';assert.throws(()=>v.build(),/REMOTE_SCOPE_INVALID/);
 }
 const v=vectors();v.remoteRows[0].deployments.mytms_build_id='DIFFERENT_PROTOCOL_BUILD';assert.throws(()=>v.build(),/DEPLOYMENT_MIXED/);
});
test('full source activation, receipt and both readiness owners contain strict operational guard; activation replay is guarded',()=>{
 for(const name of ['03_complete_operational_release_reader.sqlinc','04_complete_capability_receipt.sqlinc','05_complete_bulk_readiness.sqlinc',
  '06_complete_automatic_readiness.sqlinc','07_complete_existing_feature_setup.sqlinc']){
  const sql=read('./'+name);assert.match(sql,/operational_certificate_assert_v1|ALL_PRE_SWITCH_GATES_QUALIFIED/);
  assert.doesNotMatch(sql,/return private\.weekly_source_protected_prerequisite_release_assert_v2/);
 }
 const feature=read('./07_complete_existing_feature_setup.sqlinc');
 assert.ok(feature.indexOf('feature_request_id=v_request_id')<feature.indexOf('v_assignment.feature_receipt_json->\'protected_capability_qualification\''));
 assert.match(feature,/protected_test_setup_atomic_v1/);
});
test('both SEND7 source predicates bind database OID and the supplied exact root-owned separate turn PID',()=>{
 const sql=read('./01_complete_qualification_scope.sqlinc');
 assert.equal((sql.match(/lock_row\.database=\(select oid from pg_database where datname=current_database\(\)\)/g)||[]).length,2);
 assert.match(sql,/lock_row\.pid=\(p_request->>'turn_backend_pid'\)::integer/);
 assert.match(sql,/lock_row\.pid=\(scope_value->>'turn_backend_pid'\)::integer/);
 // Independent literal lock observations; native actual ownership proof is root-held.
 const match=(row,owned)=>row.database===owned.database&&row.pid===owned.pid&&row.pid!==owned.trialPid
  &&row.classid===20261013&&row.objid===1&&row.objsubid===2&&row.granted===true&&row.mode==='ExclusiveLock';
 const owned={database:12345,pid:101,trialPid:202},valid={database:12345,pid:101,classid:20261013,objid:1,objsubid:2,granted:true,mode:'ExclusiveLock'};
 assert.equal(match(valid,owned),true);
 for(const patch of [{database:54321},{pid:303},{granted:false},{pid:202}])assert.equal(match({...valid,...patch},owned),false);
});
test('actual Git object inventory now includes omitted workflow/toolchain/exporter/verification/security classes and modes',()=>{
 const bytes=execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','ls-tree','-r','-z','28eb91b370cc4b0dabc5424d2f40577993d0e048'],{cwd:root,encoding:null,maxBuffer:20*1024*1024});
 const rows=parseGitTree(bytes);
 for(const file of sourceInventory.required)assert.ok(rows.some(row=>row.path===file));
 assert.ok(rows.some(row=>row.path.startsWith('supabase/verification/')));assert.ok(rows.some(row=>row.path.startsWith('tests/')));
 assert.ok(rows.every(row=>['100644','100755'].includes(row.mode)&&row.type==='blob'));
});
test('real Git-object protocol mutations in every former omission and chmod/type changes are rejected',()=>{
 const revision='28eb91b370cc4b0dabc5424d2f40577993d0e048';
 const tree=parseGitTree(execFileSync('git',['ls-tree','-r','-z',revision],{cwd:root,encoding:null,maxBuffer:20*1024*1024}));
 // Restrict to one actual object from each class plus the required objects; all
 // reads remain existing committed Git objects, no repository/test mutation.
 const paths=[...sourceInventory.required,tree.find(row=>row.path.startsWith('supabase/verification/')).path,
  tree.find(row=>row.path.startsWith('tests/')).path,'supabase/release/protected-boundary-lock.json'];
 const selected=tree.filter(row=>paths.includes(row.path));
 const blobs=new Map(selected.map(row=>[row.blob,execFileSync('git',['cat-file','blob',row.blob],{cwd:root,encoding:null,maxBuffer:32*1024*1024})]));
 assert.doesNotThrow(()=>assertTreeEquivalence(selected,selected,id=>blobs.get(id)));
 for(const file of paths.filter(file=>file!==sourceInventory.manifest)){
  const changed=structuredClone(selected),row=changed.find(row=>row.path===file);row.blob='f'.repeat(40);
  assert.throws(()=>assertTreeEquivalence(selected,changed,id=>id==='f'.repeat(40)?Buffer.from('MUTATED_PROTOCOL_INPUT'):blobs.get(id)),/NATIVE_SOURCE_CHANGED/);
 }
 for(const patch of [{mode:'100755'},{type:'commit'}]){
  const changed=structuredClone(selected);Object.assign(changed.find(row=>row.path==='.node-version'),patch);
  assert.throws(()=>assertTreeEquivalence(selected,changed,id=>blobs.get(id)),/MODE_TYPE_OR_PATH_CHANGED/);
 }
});
test('release manifest can change only release identity and the closed hash-bound receipt descriptors',()=>{
 const original={formatVersion:1,releaseId:'PROTOCOL_ONLY_R1',verificationFiles:['supabase/verification/literal.sql'],standingTestAuthority:{id:'OWNER_LITERAL'}};
 const allowed={...original,releaseId:'PROTOCOL_ONLY_R2'};
 assert.equal(releaseAuthorityHash(releaseAuthority(Buffer.from(JSON.stringify(original))).authority),releaseAuthorityHash(releaseAuthority(Buffer.from(JSON.stringify(allowed))).authority));
 const changed={...allowed,verificationFiles:[]};
 assert.notEqual(releaseAuthorityHash(releaseAuthority(Buffer.from(JSON.stringify(original))).authority),releaseAuthorityHash(releaseAuthority(Buffer.from(JSON.stringify(changed))).authority));
 assert.throws(()=>releaseAuthority(Buffer.from(JSON.stringify({...allowed,weeklySourceProtectedOperational:{path:'arbitrary.json',sha256:hash}}))),/DESCRIPTOR_INVALID/);
});
test('docs-only actual checkpoints retain complete execution-tree equivalence',()=>{
 const proof=proveCommittedSources(root,'7d0cf39e8af257f93be1996a0b830d0e9a35b63e','28eb91b370cc4b0dabc5424d2f40577993d0e048');
 assert.equal(proof.schema_version,'WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2');
 assert.ok(proof.files.length>500);assert.ok(proof.files.every(row=>row.git_mode&&row.git_type&&row.execution_blob_oid));
});
test('test scope has exact legal fixture references, current installed release/expiry, no assignment mutation or automatic exemption',()=>{
 const sql=read('./TEST_JOURNEY_SCOPE.sqlinc');assert.match(sql,/CLI-913013/);assert.match(sql,/CCR-913013/);
 assert.match(sql,/interval '2 hours'/);assert.match(sql,/weekly_source_protected_qualification_release_v1/);
 assert.match(sql,/original_census,source_cas/);assert.match(sql,/original_root_identities/);
 assert.doesNotMatch(sql,/update public\.weekly_source_group_rule_assignments|set_config|grant execute|create table/i);
 assert.match(sql,/capabilities' is distinct from '\["BULK_SIGNED"\]'/);
});
test('actor is threaded through actual selection/own-stage and four full Source owners; all reverse proofs match',()=>{
 const manifest=JSON.parse(read('./OWNER_DELTAS.json'));assert.ok(manifest.proofs.every(p=>p.reverse_hunk_matches_entire_input_definition===true));
 assert.equal(manifest.new_public_rpcs,0);assert.equal(manifest.new_service_grants,0);assert.equal(manifest.new_private_signatures.length,8);
 for(const file of ['10_complete_actor_bulk_selection.sqlinc','11_complete_actor_own_stage.sqlinc','12_complete_actor_17_prepare_family.sqlinc',
  '12_complete_actor_18_prepare_action.sqlinc','12_complete_actor_19_action_context.sqlinc','12_complete_actor_20_stage_request.sqlinc']){
  assert.match(read('./'+file),/cas_assert_v1\([^\n]*p_actor|non_self_bill_mode_v1\([^\n]*v_actor/);
 }
 const hunk=JSON.parse(read('./INDEX_CALL_SITE_DELTAS.json'));assert.equal(hunk.reverse_hunk_matches_entire_input_definition,true);
 assert.match(hunk.hunks[1].after,/actorUserId: actor.id/);assert.doesNotMatch(hunk.hunks[1].after,/producer_ready.*true|qualification.*env/i);
});
test('direct normalized managed q cannot bypass constructor provenance; altered receipt/deployment/closure fields refuse',()=>{
 const managedContext={environment:'TEST',expectedDatabase:'cloudtms_test_clone',releaseId:context.release_id,gitCommit:context.git_commit,expectedHash:hash};
 for(const [mutate,expected]of [
  [q=>{q.remote_gate_receipts[0].receipt_id='';},/REMOTE_SCOPE_INVALID/],
  [q=>{q.remote_gate_receipts[0].server_request_ids=[];},/DEPLOYMENT_INVALID/],
  [q=>{q.remote_gate_receipts[0].deployments.worker_version_id='invalid';},/DEPLOYMENT_INVALID/],
  [q=>{q.remote_gate_receipts[0].deployments.office_bundle_sha256='invalid';},/DEPLOYMENT_INVALID/],
  [q=>{q.remote_gate_receipts[0].deployments.extra='unsupported';},/DEPLOYMENT_INVALID/],
  [q=>{q.test_setup_closure.actor_user_id='invalid';},/SETUP_CLOSURE_INVALID/],
  [q=>{q.test_setup_closure.week_ending='invalid';},/SETUP_CLOSURE_INVALID/],
  [q=>{q.test_setup_closure.closed_at_utc='2026';},/SETUP_CLOSURE_INVALID/],
  [q=>{q.test_setup_closure.closure_audit_sha256='invalid';},/SETUP_CLOSURE_INVALID/]
 ]){
  const q=vectors().build();mutate(q);const {qualification_sha256,...body}=q;q.qualification_sha256=domainHash(q.schema_version,body);
  assert.throws(()=>assertManagedOperationalEvidence(managedContext,{weekly_source_protected_capability_qualifications:[q]},root),expected);
 }
 const sql=read('./OPERATIONAL_CERTIFICATE.sqlinc');
 assert.match(sql,/qualification_closed_object_v1\(closure,array/);assert.match(sql,/qualification_closed_object_v1\(row_value,array/);
 assert.match(sql,/qualification_closed_object_v1\(row_value->'deployments',array/);
 assert.match(sql,/PostgreSQL cannot read Git objects/);
});
