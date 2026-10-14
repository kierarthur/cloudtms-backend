import {execFileSync} from 'node:child_process';
import {exact,domainHash,sha256} from '../protected-source-capability/release-qualification.mjs';
import {sourceInventory,proveCommittedSources} from '../strict-qualification-git-path-parser-forward/source-equivalence.mjs';
import {buildOffRuleTestAuthorization,assertManagedOffRuleTestEvidence} from './off-rule-setup-evidence.mjs';
import {buildMeasuredNativePartial} from './measured-partial-evidence.mjs';
import {buildOperationalQualification,assertManagedOperationalEvidence} from './final-operational-evidence.mjs';
import {buildActivationProbeAuthorization} from './activation-probe-evidence.mjs';
import {buildPrerequisiteQualification} from './same-installed-prerequisite-evidence.mjs';
import {buildTestSetupAuthorization} from '../protected-source-qualification-operational-gate/setup-evidence.mjs';
const fail=code=>{throw Error(code);},hex=/^[0-9a-f]{64}$/,commit=/^[0-9a-f]{40}$/,
 uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
export const receiptKeys=Object.freeze(['schema_version','execution','receipt_input_commit','receipt_tree_sha256','artifact_slot','artifacts_sha256',
 'prior_evidence_sha256','evidence_sha256','actor_user_id','recorded_at_utc','replayed','operational_settings_changed','receipt_sha256']);
const selection=Object.freeze({
 weekly_source_protected_test_setup_authorizations:['weeklySourceProtectedTestSetup','authorizations'],
 weekly_source_off_rule_test_controls:['weeklySourceProtectedTestSetup','controls'],
 weekly_source_protected_partial_native_evidence:['weeklySourceProtectedPrerequisites','partial_native_evidence'],
 weekly_source_protected_activation_probe_authorizations:['weeklySourceProtectedPrerequisites','activation_probe_authorizations'],
 weekly_source_protected_capability_qualifications:['weeklySourceProtectedOperational','qualifications'],
 weekly_source_protected_prerequisite_qualifications:['weeklySourceProtectedPrerequisites','qualifications']
});
const bundleKeys=Object.freeze({
 weeklySourceProtectedTestSetup:['schema_version','authorizations','controls'],
 weeklySourceProtectedPrerequisites:['schema_version','qualifications','partial_native_evidence','activation_probe_authorizations'],
 weeklySourceProtectedOperational:['schema_version','qualifications','report_fin_qualifications','report_fin_native_inputs','report_fin_native_artifacts']
});
const bundleSchemas=Object.freeze({weeklySourceProtectedTestSetup:'WEEKLY_SOURCE_PROTECTED_TEST_SETUP_INPUT_BUNDLE_V1',
 weeklySourceProtectedPrerequisites:'WEEKLY_SOURCE_PROTECTED_PREREQUISITE_INPUT_BUNDLE_V1',
 weeklySourceProtectedOperational:'WEEKLY_SOURCE_PROTECTED_OPERATIONAL_INPUT_BUNDLE_V1'});
function jsonBytes(bytes){let s;try{s=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);}catch{fail('POST_VERIFIED_ARTIFACT_UTF8_INVALID');}
 if(!Buffer.from(s).equals(bytes))fail('POST_VERIFIED_ARTIFACT_UTF8_INVALID');return JSON.parse(s);}
function committedJson(root,revision,file){
 if(!commit.test(revision)||!Object.values(sourceInventory.receiptBundles).includes(file)&&file!==sourceInventory.manifest)fail('POST_VERIFIED_SOURCE_INVALID');
 const bytes=execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',revision+':'+file],{cwd:root,encoding:null,maxBuffer:32*1024*1024});
 return {bytes,value:jsonBytes(bytes)};
}
function unpack(inputs){if(!Array.isArray(inputs))fail('POST_VERIFIED_RAW_GATE_INPUT_REQUIRED');return inputs.map(input=>{
 exact(input,['gate','receipt_id','receipt','receipt_sha256'],'POST_VERIFIED_RAW_GATE_INPUT_REQUIRED');
 if(typeof input.receipt!=='string'||sha256(input.receipt)!==input.receipt_sha256)fail('POST_VERIFIED_RAW_GATE_INPUT_REQUIRED');
 return {gate:input.gate,receipt_id:input.receipt_id,receipt_bytes:Buffer.from(input.receipt),expected_receipt_sha256:input.receipt_sha256};
 });}
export function assertPostVerifiedReceipt(receipt,request,actor){
 exact(receipt,receiptKeys,'POST_VERIFIED_RECEIPT_INVALID');const {receipt_sha256,...body}=receipt;
 if(receipt.schema_version!=='WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1'||receipt.actor_user_id!==actor
  ||JSON.stringify(receipt.execution)!==JSON.stringify(request.expected_execution)||receipt.receipt_input_commit!==request.receipt_input_commit
  ||receipt.receipt_tree_sha256!==request.receipt_tree_proof.source_equivalence_sha256||receipt.artifact_slot!==request.artifact_slot
  ||receipt.artifacts_sha256!==domainHash('WEEKLY_SOURCE_POST_VERIFIED_ARTIFACT_SET_V1',request.artifacts)
  ||receipt.prior_evidence_sha256!==request.expected_evidence_sha256||!hex.test(receipt.evidence_sha256)
  ||receipt.operational_settings_changed!==false||typeof receipt.replayed!=='boolean'
  ||typeof receipt.recorded_at_utc!=='string'||!Number.isFinite(Date.parse(receipt.recorded_at_utc))
  ||domainHash(receipt.schema_version,body)!==receipt_sha256)fail('POST_VERIFIED_RECEIPT_INVALID');
 return receipt;
}
// Pure source preparation. The execution DTO must come from the actual native
// qualification release reader under the root-owned connection/turn below.
// Every byte is re-read from Git, not from arbitrary caller-normalized q JSON.
export function preparePostVerifiedRequest({root,execution,actorUserId,expectedEvidenceSha256,receiptInputCommit,artifactSlot,reason,turnBackendPid}){
 exact(execution,['release_id','source_commit','contract_sha256','installation_id','environment','database','managed_manifest_sha256','installed_catalogue_jsonb_sha256'],
  'POST_VERIFIED_EXECUTION_INVALID');
 if(execution.environment!=='TEST'||execution.database!=='cloudtms_test_clone'||!commit.test(execution.source_commit)
  ||!hex.test(execution.contract_sha256)||!uuid.test(execution.installation_id)||!uuid.test(actorUserId)||!hex.test(expectedEvidenceSha256)
  ||!commit.test(receiptInputCommit)||receiptInputCommit===execution.source_commit||!selection[artifactSlot]
  ||typeof reason!=='string'||reason.length<1||reason.length>1000||!Number.isSafeInteger(turnBackendPid)||turnBackendPid<1)
  fail('POST_VERIFIED_REQUEST_INVALID');
 const proof=proveCommittedSources(root,execution.source_commit,receiptInputCommit);
 const {value:manifest}=committedJson(root,receiptInputCommit,sourceInventory.manifest);
 // This commit records receipt data. It cannot claim to be another installed
 // release, even though the generic historical tree proof permits that field.
 if(manifest.releaseId!==execution.release_id)fail('POST_VERIFIED_RELEASE_ID_RELABELLED');
 const readBundle=key=>{
  const descriptor=manifest[key];exact(descriptor,['path','sha256'],'POST_VERIFIED_BUNDLE_DESCRIPTOR_INVALID');
  if(descriptor.path!==sourceInventory.receiptBundles[key]||!hex.test(descriptor.sha256))fail('POST_VERIFIED_BUNDLE_DESCRIPTOR_INVALID');
  const {value,bytes}=committedJson(root,receiptInputCommit,descriptor.path);
  if(sha256(bytes)!==descriptor.sha256||value.schema_version!==bundleSchemas[key])fail('POST_VERIFIED_BUNDLE_INVALID');
  exact(value,bundleKeys[key].filter(k=>k==='schema_version'||value[k]!==undefined),'POST_VERIFIED_BUNDLE_INVALID');return value;
 };
 const [bundleKey,member]=selection[artifactSlot],bundle=readBundle(bundleKey);
 const rows=bundle[member];if(!Array.isArray(rows)||rows.length<1||rows.length>100)fail('POST_VERIFIED_BUNDLE_INVALID');
 const context={environment:'TEST',expectedDatabase:execution.database,mode:'UPGRADE',releaseId:execution.release_id,
  gitCommit:execution.source_commit,expectedHash:execution.contract_sha256};
 const preparePrerequisites=rows=>{
  if(!Array.isArray(rows)||rows.length<1||rows.length>100)fail('POST_VERIFIED_PREREQUISITE_INPUT_INVALID');
  return rows.map(row=>{
   exact(row,['agency_id','source_group_id','effective_from_week_ending','effective_to_week_ending','capabilities','gate_inputs'],'POST_VERIFIED_PREREQUISITE_INPUT_INVALID');
   if(JSON.stringify(row.capabilities)!=='["BULK_SIGNED"]')fail('POST_VERIFIED_PREREQUISITE_SCOPE_REFUSED');
   const scope={release_id:execution.release_id,git_commit:execution.source_commit,contract_sha256:execution.contract_sha256,
    agency_id:row.agency_id,environment:'TEST',database:execution.database,source_group_id:row.source_group_id,
    effective_from_week_ending:row.effective_from_week_ending,effective_to_week_ending:row.effective_to_week_ending,capabilities:row.capabilities};
   return buildPrerequisiteQualification(scope,unpack(row.gate_inputs),proveCommittedSources(root,execution.source_commit,execution.source_commit));
  });
 };
 let artifacts;
 if(artifactSlot==='weekly_source_off_rule_test_controls'){
  if(rows.length!==1)fail('POST_VERIFIED_CONTROLS_INVALID');artifacts=rows;
  // These controls are a reviewed input, not operational qualification. The
  // same SQL owner independently binds the installed identities/native rows.
  exact(rows[0],['schema_version','release_id','source_commit','contract_sha256','banking_install_receipt_sha256','security_receipt_sha256',
   'source_effects_review_sha256','banking_first_install_complete','effective_acl_pass','browser_denied','source_effects_no_external_calls'],'POST_VERIFIED_CONTROLS_INVALID');
  if(rows[0].release_id!==execution.release_id||rows[0].source_commit!==execution.source_commit||rows[0].contract_sha256!==execution.contract_sha256
   ||rows[0].schema_version!=='WEEKLY_SOURCE_OFF_RULE_TEST_CONTROLS_V1'
   ||!['banking_install_receipt_sha256','security_receipt_sha256','source_effects_review_sha256'].every(k=>hex.test(rows[0][k]))
   ||!['banking_first_install_complete','effective_acl_pass','browser_denied','source_effects_no_external_calls'].every(k=>rows[0][k]===true))fail('POST_VERIFIED_CONTROLS_INVALID');
 }else if(artifactSlot==='weekly_source_protected_test_setup_authorizations'){
  artifacts=rows.map(row=>{
   if(Object.hasOwn(row,'purpose')){
    if(!Array.isArray(bundle.controls)||bundle.controls.length!==1)fail('POST_VERIFIED_CONTROLS_INVALID');
    return buildOffRuleTestAuthorization(context,row,bundle.controls[0]);
   }
   return buildTestSetupAuthorization(context,row,preparePrerequisites(readBundle('weeklySourceProtectedPrerequisites').qualifications));
  });
  assertManagedOffRuleTestEvidence(context,{weekly_source_off_rule_test_controls:bundle.controls?.[0],weekly_source_protected_test_setup_authorizations:artifacts});
 }else if(artifactSlot==='weekly_source_protected_prerequisite_qualifications'){
  artifacts=preparePrerequisites(rows);
 }else if(artifactSlot==='weekly_source_protected_partial_native_evidence'){
  artifacts=rows.map(row=>{exact(row,['native_context','native_gate_inputs'],'POST_VERIFIED_PARTIAL_INPUT_INVALID');
   if(row.native_context.release_id!==execution.release_id||row.native_context.git_commit!==execution.source_commit)fail('POST_VERIFIED_EXECUTION_RELABELLED');
   return buildMeasuredNativePartial(row.native_context,unpack(row.native_gate_inputs));});
 }else if(artifactSlot==='weekly_source_protected_activation_probe_authorizations'){
  artifacts=rows.map(row=>{exact(row,['context','native_context','native_gate_inputs','security_input','remote_gate_inputs','setup_closure','assignment_scope'],
    'POST_VERIFIED_PROBE_INPUT_INVALID');
   if(row.context.release_id!==execution.release_id||row.context.git_commit!==execution.source_commit
    ||row.native_context.release_id!==execution.release_id||row.native_context.git_commit!==execution.source_commit)fail('POST_VERIFIED_EXECUTION_RELABELLED');
   // Probe constructors currently use the actual installed source proof. The
   // recorder additionally binds the full installed->receipt-input proof.
   return buildActivationProbeAuthorization(row.context,row.native_context,unpack(row.native_gate_inputs),unpack([row.security_input])[0],
    unpack(row.remote_gate_inputs),row.setup_closure,proveCommittedSources(root,execution.source_commit,execution.source_commit),row.assignment_scope);});
 }else{
  artifacts=rows.map(row=>{exact(row,['context','native_context','native_gate_inputs','remote_gate_inputs','setup_closure','activation_scope'],
    'POST_VERIFIED_OPERATIONAL_INPUT_INVALID');
   if(row.context.release_id!==execution.release_id||row.context.git_commit!==execution.source_commit
    ||row.native_context.release_id!==execution.release_id||row.native_context.git_commit!==execution.source_commit)fail('POST_VERIFIED_EXECUTION_RELABELLED');
   return buildOperationalQualification(row.context,row.native_context,unpack(row.native_gate_inputs),unpack(row.remote_gate_inputs),row.setup_closure,proof,row.activation_scope);});
  assertManagedOperationalEvidence(context,{weekly_source_protected_capability_qualifications:artifacts},root);
 }
 return {schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECORD_V1',expected_execution:execution,
  expected_evidence_sha256:expectedEvidenceSha256,receipt_input_commit:receiptInputCommit,receipt_tree_proof:proof,
  artifact_slot:artifactSlot,artifacts,reason,turn_backend_pid:turnBackendPid};
}
const literal=value=>"'"+String(value).replaceAll("'","''")+"'";
// Called only by root's protected owner runner with its real managed session.
// It does not open a connection, take another account credential, deploy, or
// execute when imported. The parent owns the separate SEND7 lock connection.
export async function recordPostVerifiedEvidence({session,root,actorUserId,receiptInputCommit,artifactSlot,reason,turnBackendPid}){
 if(!session||typeof session.execute!=='function')fail('POST_VERIFIED_OWNER_SESSION_REQUIRED');
 let committed=false;
 try{
  const held=await session.execute("select pg_try_advisory_lock(hashtextextended('cloudtms_database_release_admission_v1',0));");
  if(held.trim()!=='t')fail('POST_VERIFIED_MANAGED_WRITER_BUSY');
  await session.execute("begin; set local lock_timeout='5s'; set local statement_timeout='120s'; select private.weekly_source_protected_qualification_owner_v1();");
  const state=JSON.parse(await session.execute("select jsonb_build_object('execution',private.weekly_source_protected_qualification_release_v1(),"
   +"'evidence_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_RELEASE_EVIDENCE_V1',r.evidence_json),'hex'))::text "
   +"from private.cloudtms_database_releases r order by started_at_utc desc,release_id desc limit 1;"));
  exact(state,['execution','evidence_sha256'],'POST_VERIFIED_EXECUTION_INVALID');
  const request=preparePostVerifiedRequest({root,execution:state.execution,actorUserId,expectedEvidenceSha256:state.evidence_sha256,
   receiptInputCommit,artifactSlot,reason,turnBackendPid});
  const receipt=JSON.parse(await session.execute('select private.weekly_source_post_verified_evidence_record_v1('
   +literal(actorUserId)+'::uuid,'+literal(JSON.stringify(request))+'::jsonb)::text;'));
  assertPostVerifiedReceipt(receipt,request,actorUserId);
  await session.execute('commit;');committed=true;return receipt;
 }finally{
  if(!committed)try{await session.execute('rollback;');}catch{}
  // The owner runner closes its own session if the connection fails; no new
  // unguarded writer is opened to repair or retry an uncertain metadata write.
  try{await session.execute("select pg_advisory_unlock(hashtextextended('cloudtms_database_release_admission_v1',0));");}catch{}
 }
}
