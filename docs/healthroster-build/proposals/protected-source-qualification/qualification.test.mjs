import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import {fileURLToPath} from 'node:url';
import {buildPrerequisiteQualification,prerequisiteGates,requiredOperationalReplay,proveCommittedSources,installedQualificationCatalogueEvidence} from './prerequisite-evidence.mjs';
import {domainHash} from '../protected-source-capability/release-qualification.mjs';
const sha=x=>crypto.createHash('sha256').update(x).digest('hex'),id='00110011-0011-4011-8011-001100110011',hash='1'.repeat(64);
// Synthetic protocol vectors ONLY. No fixture below is an acceptance receipt,
// no serialized qualification result is emitted or installed by these tests.
const context={release_id:'PROTOCOL_ONLY_R2',git_commit:'2'.repeat(40),contract_sha256:hash,agency_id:id,
 environment:'TEST',database:'cloudtms_test_clone',source_group_id:id,effective_from_week_ending:'2026-10-18',
 effective_to_week_ending:null,capabilities:['BULK_SIGNED']};
const execution={release_id:'PROTOCOL_ONLY_R1',source_commit:'1'.repeat(40),contract_sha256:hash,installation_id:id,
 agency_id:id,environment:'TEST',database:'cloudtms_test_clone',managed_manifest_sha256:hash,installed_catalogue_jsonb_sha256:hash};
function vectors(){
 const rows=Object.keys(prerequisiteGates).filter(g=>g!=='AUTOMATIC_COMPONENT_DELTA').map(gate=>({schema_version:'WEEKLY_SOURCE_TEST_ROLLBACK_PREREQUISITE_RECEIPT_V1',
  gate,execution:{...execution},source_group_id:id,source_cycle_id:id,week_ending:'2026-10-18',effective_from_week_ending:'2026-10-18',effective_to_week_ending:null,
  scope_id:id,scope_sha256:hash,transaction_id:'101',original_census_sha256:hash,source_cas_sha256:hash,
  native_execution_receipt:{accepted:true,database:'cloudtms_test_clone',rolledBack:true,connectedFresh:true,
   tool_receipt_sha256:hash,native_journey_id:'PROTECTED_PREREQUISITE_'+gate,scope_audit_rolled_back:true},
  assertions:Object.fromEntries(prerequisiteGates[gate].map(key=>[key,true]))}));
 const pack=()=>rows.map(row=>{const bytes=Buffer.from(JSON.stringify(row));return {gate:row.gate,receipt_id:'PROTOCOL_ONLY/'+row.gate,
  receipt_bytes:bytes,expected_receipt_sha256:sha(bytes)};});
 const sourceBody={schema_version:'WEEKLY_SOURCE_COMMITTED_NATIVE_SOURCE_EQUIVALENCE_V1',execution_source_commit:execution.source_commit,
  installation_source_commit:context.git_commit,files:[{path:'broker/src/index.js',sha256:hash}]};
 const source={...sourceBody,source_equivalence_sha256:domainHash(sourceBody.schema_version,sourceBody)};
 return {rows,pack,source};
}
test('protocol keeps original R1 identity and distinguishes prerequisite from full operational PASS',()=>{
 const v=vectors(),q=buildPrerequisiteQualification(context,v.pack(),v.source);
 assert.equal(q.execution_identity.release_id,'PROTOCOL_ONLY_R1');assert.equal(q.release_id,'PROTOCOL_ONLY_R2');
 assert.equal(q.phase,'PREREQUISITES_QUALIFIED_OPERATIONAL_REPLAY_PENDING');
 assert.deepEqual(q.required_operational_replay,requiredOperationalReplay);
 assert.equal(q.gate_receipts.length,9);assert.ok(q.gate_receipts.every(r=>r.execution_release_id==='PROTOCOL_ONLY_R1'&&r.status==='PREREQUISITE_PASS'));
});
test('cannot relabel execution as the future release',()=>{
 const v=vectors();v.rows[0].execution.release_id=context.release_id;
 assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/EXECUTION_INVALID/);
});
test('contract or installation/database/agency identity mismatch refuses',()=>{
 for(const [field,value] of [['contract_sha256','3'.repeat(64)],['database','foreign_database'],['agency_id','00220022-0022-4022-8022-002200220022']]){
  const v=vectors();v.rows[0].execution[field]=value;
  assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/EXECUTION_INVALID/);
 }
});
test('mixed verified execution releases cannot be combined',()=>{
 const v=vectors();v.rows[1].execution.release_id='OTHER_R1';
 assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/EXECUTION_MIXED/);
});
test('missing, duplicated and definitions-only native gates refuse',()=>{
 const v=vectors(),inputs=v.pack();assert.throws(()=>buildPrerequisiteQualification(context,inputs.slice(1),v.source),/GATE_CENSUS/);
 inputs[0]=inputs[1];assert.throws(()=>buildPrerequisiteQualification(context,inputs,v.source),/GATE_CENSUS/);
 v.rows[0].native_execution_receipt.accepted=false;assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/NATIVE_EXECUTION/);
});
test('every single failed assertion and absent fresh rollback proof refuse',()=>{
 for(const gate of Object.keys(prerequisiteGates).filter(g=>g!=='AUTOMATIC_COMPONENT_DELTA')){
  const v=vectors(),row=v.rows.find(r=>r.gate===gate);row.assertions[prerequisiteGates[gate][0]]=false;
  assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/ASSERTION_FAILED/);
 }
 for(const field of ['rolledBack','connectedFresh','scope_audit_rolled_back']){
  const v=vectors();v.rows[0].native_execution_receipt[field]=false;
  assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/NATIVE_EXECUTION/);
 }
});
test('R1 commit is preserved; foreign source-equivalence commit or hash refuses',()=>{
 const v=vectors();v.source.execution_source_commit='7'.repeat(40);
 assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/SOURCE_PROOF/);
 const v2=vectors();v2.source.source_equivalence_sha256='8'.repeat(64);
 assert.throws(()=>buildPrerequisiteQualification(context,v2.pack(),v2.source),/SOURCE_PROOF/);
});
test('a lawful old NULL-rule Sunday cannot be retagged into a future enabled range',()=>{
 const v=vectors();v.rows[0].week_ending='2026-10-11';
 assert.throws(()=>buildPrerequisiteQualification(context,v.pack(),v.source),/NATIVE_SCOPE/);
});
test('catalogue binding uses actual complete verified contract metadata, not a VERIFIED string',()=>{
 const actual={contract_version:5,routines:[{schema:'private',identity:'owner()',definition_sha256:hash}],triggers:[]};
 const evidence=installedQualificationCatalogueEvidence(actual,sha(JSON.stringify(actual)));
 assert.equal(evidence.schema_version,'WEEKLY_SOURCE_QUALIFICATION_INSTALLED_CATALOGUE_V1');
 const changed=structuredClone(actual);changed.routines[0].definition_sha256='3'.repeat(64);
 assert.throws(()=>installedQualificationCatalogueEvidence(changed,sha(JSON.stringify(actual))),/INSTALLED_CONTRACT_MISMATCH/);
 assert.notEqual(evidence.jsonb_sha256,installedQualificationCatalogueEvidence(changed,sha(JSON.stringify(changed))).jsonb_sha256);
});
test('actual committed runtime file census permits documentation-only checkpoint identity change',()=>{
 const root=fileURLToPath(new URL('../../../../',import.meta.url));
 const proof=proveCommittedSources(root,'7d0cf39e8af257f93be1996a0b830d0e9a35b63e','8d879305d9d63a444bceed1df7afdc6884a1ac21');
 assert.ok(proof.files.length>500);assert.ok(proof.files.some(f=>f.path==='broker/src/index.js'));
 assert.ok(proof.files.some(f=>f.path.startsWith('supabase/repeatable/')));
 assert.ok(proof.files.every(f=>!f.path.startsWith('docs/')));
 assert.notEqual(proof.execution_source_commit,proof.installation_source_commit);
});
test('complete source deltas retain native bodies through exact reverse hunks',()=>{
 const receipt=JSON.parse(fs.readFileSync(new URL('./QUALIFICATION_NATIVE_OWNER_DELTA.json',import.meta.url)));
 assert.equal(receipt.new_public_rpcs,0);assert.equal(receipt.new_service_grants,0);
 assert.ok(receipt.proofs.filter(p=>p.name.startsWith('private.')).every(p=>p.reverse_hunk_matches_entire_input_definition===true));
 const sql=fs.readFileSync(new URL('./QUALIFICATION_SCOPE.sqlinc',import.meta.url),'utf8');
 assert.match(sql,/session_user is distinct from owner_name/);assert.match(sql,/get diagnostics context_value=pg_context/);
 assert.match(sql,/pg_current_xact_id\(\)::text/);assert.match(sql,/operational_ready',false/);
 assert.doesNotMatch(sql,/set_config|create table|alter table|grant execute/i);
 const step=fs.readFileSync(new URL('./QUALIFICATION_STEP.sqlinc',import.meta.url),'utf8');
 assert.doesNotMatch(step,/\bexecute\s*'/i);assert.match(step,/max_native_steps/);
 assert.match(step,/weekly_exceptional_pay_complete_local_v1\(request_value\)/);
});
