import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {proveCommittedSources} from './source-equivalence.mjs';
import {exact,domainHash,validateQualificationContext,assertManagedQualificationEvidence as assertLegacy} from '../protected-source-capability/release-qualification.mjs';
const hex=/^[0-9a-f]{64}$/,commit=/^[0-9a-f]{40}$/,uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const fail=code=>{throw Error(code);},sha=x=>crypto.createHash('sha256').update(x).digest('hex');
// Schema/security contract metadata only; no monetary JSON serialization.
function contractJsonbText(value){
 if(value===null||typeof value==='boolean'||typeof value==='string')return JSON.stringify(value);
 if(typeof value==='number'&&Number.isSafeInteger(value))return String(value);
 if(Array.isArray(value))return '['+value.map(contractJsonbText).join(', ')+']';
 if(value&&typeof value==='object')return '{'+Object.keys(value).sort((a,b)=>Buffer.byteLength(a)-Buffer.byteLength(b)
  ||Buffer.compare(Buffer.from(a),Buffer.from(b))).map(key=>JSON.stringify(key)+': '+contractJsonbText(value[key])).join(', ')+'}';
 fail('PREREQUISITE_CATALOGUE_METADATA_INVALID');
}
export function installedQualificationCatalogueEvidence(actualContract,expectedHash){
 if(!hex.test(expectedHash)||sha(JSON.stringify(actualContract))!==expectedHash)fail('PREREQUISITE_INSTALLED_CONTRACT_MISMATCH');
 return {schema_version:'WEEKLY_SOURCE_QUALIFICATION_INSTALLED_CATALOGUE_V1',contract_sha256:expectedHash,
  jsonb_sha256:sha('WEEKLY_SOURCE_QUALIFICATION_INSTALLED_CATALOGUE_V1'+'\x1f'+contractJsonbText(actualContract))};
}
export const prerequisiteBundlePath='supabase/release/weekly-source-protected-prerequisites.json';
// This input has no future R2 identity. The protected source-gated release
// supplies that identity after checkout. No CLI/env/browser switch is added.
export function loadCommittedPrerequisiteJourneys(root,release,context){
 const descriptor=release.weeklySourceProtectedPrerequisites;if(descriptor===undefined)return [];
 if(context.environment!=='TEST'||context.mode!=='UPGRADE'||context.expectedDatabase!=='cloudtms_test_clone')fail('PREREQUISITE_MANAGED_SCOPE_INVALID');
 exact(descriptor,['path','sha256'],'PREREQUISITE_BUNDLE_DESCRIPTOR_INVALID');
 if(descriptor.path!==prerequisiteBundlePath||!hex.test(descriptor.sha256))fail('PREREQUISITE_BUNDLE_DESCRIPTOR_INVALID');
 const bytes=fs.readFileSync(path.join(root,prerequisiteBundlePath));
 const committed=execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',context.gitCommit+':'+prerequisiteBundlePath],
  {cwd:root,encoding:null,maxBuffer:2*1024*1024});
 if(bytes.length>2*1024*1024||sha(bytes)!==descriptor.sha256||!bytes.equals(committed))fail('PREREQUISITE_BUNDLE_SOURCE_MISMATCH');
 const bundle=JSON.parse(bytes.toString('utf8'));
 exact(bundle,['schema_version','qualifications'],'PREREQUISITE_BUNDLE_INVALID');
 if(bundle.schema_version!=='WEEKLY_SOURCE_PROTECTED_PREREQUISITE_INPUT_BUNDLE_V1'
  ||!Array.isArray(bundle.qualifications)||!bundle.qualifications.length||bundle.qualifications.length>100)fail('PREREQUISITE_BUNDLE_INVALID');
 return bundle.qualifications.map(row=>{
  exact(row,['agency_id','source_group_id','effective_from_week_ending','effective_to_week_ending','capabilities','gate_inputs'],'PREREQUISITE_BUNDLE_SCOPE_INVALID');
  const qcontext={release_id:context.releaseId,git_commit:context.gitCommit,contract_sha256:context.expectedHash,
   agency_id:row.agency_id,environment:context.environment,database:context.expectedDatabase,source_group_id:row.source_group_id,
   effective_from_week_ending:row.effective_from_week_ending,effective_to_week_ending:row.effective_to_week_ending,capabilities:row.capabilities};
  validateQualificationContext(qcontext);
  if(!Array.isArray(row.gate_inputs))fail('PREREQUISITE_GATE_CENSUS_INVALID');
  const receipts=row.gate_inputs.map(input=>{
   exact(input,['gate','receipt_id','receipt','receipt_sha256'],'PREREQUISITE_BUNDLE_GATE_INVALID');
   // The root collector writes this canonical sanitised receipt byte string.
   // It preserves the real native envelope hash, not raw diagnostics/secrets.
   if(typeof input.receipt!=='string'||sha(input.receipt)!==input.receipt_sha256)fail('PREREQUISITE_ARTIFACT_INVALID');
   return {gate:input.gate,receipt_id:input.receipt_id,receipt_bytes:input.receipt,expected_receipt_sha256:input.receipt_sha256};
  });
  if(!receipts.length)fail('PREREQUISITE_GATE_CENSUS_INVALID');
  const first=JSON.parse(receipts[0].receipt_bytes);
  const proof=proveCommittedSources(root,first.execution.source_commit,context.gitCommit);
  buildPrerequisiteQualification(qcontext,receipts,proof); // Before any durable managed writer call.
  return {phase:'ROLLBACK_PREREQUISITE',context:qcontext,receipts};
 });
}
// Complete literal execution tree inventory (mode/type/workflow/release/verifiers).
export { proveCommittedSources } from './source-equivalence.mjs';
// Honest prerequisite assertions. These are NOT aliases for the old full
// operational gates. Post-activation API/UI/activation acceptance stays owed.
export const prerequisiteGates=Object.freeze({
 SOURCE_CAS_NATIVE:['sorted_original_clients','all_39_categories_rederived','remaining_client_retained','source_change_40001','own_stage_cas_unchanged'],
 NATIVE_PROTECTION:['genuine_signed_claim','native_prepare_stage_complete','same_family_rounds','cross_candidate_rounds','partial_retry_native_receipt'],
 SECURITY:['private_scope_owner_only','browser_and_service_scope_denied','revoked_actor_refused','foreign_scope_refused','operational_ready_stays_false'],
 LEGACY_PAY:['actual_legacy_owner','native_source_pay_identity_retained','actual_projection_amounts','no_payment_provider_calls','no_external_effects'],
 NEXT_PAY:['actual_published_next_installation','native_i1_complete_set','candidate_gate_first','empty_and_growth_40001','native_hooks_retained'],
 CLIENT_API:['trusted_actor_core','closed_native_payload','native_preview_admit_round_apply','native_partial_retry_shape','source_cas_error_order','full_http_replay_not_claimed'],
 DOCUMENT:['genuine_source_bytes','native_snapshot_model','frozen_final_issue_hash','actual_membership_and_zero_authority','native_renderer_qualification','no_network_delivery'],
 OWN_STAGE_CAS:['genuine_pending_native_request','only_exact_own_stage_excluded','tamper_and_foreign_stage_refused','completed_client_retained','all_39_source_categories_unchanged'],
 SOURCE_ROUTING:['actual_legal_non_self_bill_policy','null_rules_unchanged','nhsp_excluded','signed_native_guards_retained','canonical_original_pay_only'],
 AUTOMATIC_COMPONENT_DELTA:['actual_enabled_s18b_rule','literal_complete_component_census','actual_staged_before_after','money_and_structure_zero','mismatch_manual_only','immutable_replay_identity']
});
export const requiredOperationalReplay=Object.freeze([
 'CONTROLLED_ACTIVATION_POSITIVE_NEGATIVE_CAS','REAL_CLIENT_API_PREVIEW_COMMIT_RETRY_ERROR_ORDER',
 'REAL_OFFICE_UI_DEEP_LINK_ACTIONS','REAL_MYTMS_SIGNED_DOCUMENT_AND_APP',
 'REAL_POST_ACTIVATION_NATIVE_LEGACY_NEXT_REPLAY','REAL_POST_ACTIVATION_SOURCE_ROUTING']);
const executionKeys=['release_id','source_commit','contract_sha256','installation_id','agency_id','environment','database',
 'managed_manifest_sha256','installed_catalogue_jsonb_sha256'];
function validateExecution(value,context){
 exact(value,executionKeys,'PREREQUISITE_EXECUTION_INVALID');
 if(typeof value.release_id!=='string'||!value.release_id||value.release_id===context.release_id
  ||!commit.test(value.source_commit)||!hex.test(value.contract_sha256)||!uuid.test(value.installation_id)
  ||value.contract_sha256!==context.contract_sha256||value.agency_id!==context.agency_id
  ||!hex.test(value.managed_manifest_sha256)||!hex.test(value.installed_catalogue_jsonb_sha256)
  ||value.environment!=='TEST'||value.database!=='cloudtms_test_clone')fail('PREREQUISITE_EXECUTION_INVALID');
}
export function buildPrerequisiteQualification(context,inputs,sourceProof){
 validateQualificationContext(context);
 const needed=Object.keys(prerequisiteGates).filter(g=>g!=='AUTOMATIC_COMPONENT_DELTA'||context.capabilities.includes('AUTOMATIC_RECONCILE'));
 if(!Array.isArray(inputs)||inputs.length!==needed.length||new Set(inputs.map(i=>i.gate)).size!==needed.length)fail('PREREQUISITE_GATE_CENSUS_INVALID');
 let executionIdentity;
 const rows=inputs.map(input=>{
  exact(input,['gate','receipt_id','receipt_bytes','expected_receipt_sha256'],'PREREQUISITE_INPUT_INVALID');
  if(!needed.includes(input.gate)||typeof input.receipt_id!=='string'||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(input.receipt_id)
   ||!(Buffer.isBuffer(input.receipt_bytes)||typeof input.receipt_bytes==='string')||sha(input.receipt_bytes)!==input.expected_receipt_sha256)fail('PREREQUISITE_ARTIFACT_INVALID');
  const receipt=JSON.parse(input.receipt_bytes.toString('utf8'));
  exact(receipt,['schema_version','gate','execution','source_group_id','source_cycle_id','week_ending','effective_from_week_ending','effective_to_week_ending',
   'scope_id','scope_sha256','transaction_id','original_census_sha256','source_cas_sha256','native_execution_receipt','assertions'],'PREREQUISITE_NATIVE_RECEIPT_INVALID');
  if(receipt.schema_version!=='WEEKLY_SOURCE_TEST_ROLLBACK_PREREQUISITE_RECEIPT_V1'||receipt.gate!==input.gate
   ||receipt.source_group_id!==context.source_group_id||receipt.effective_from_week_ending!==context.effective_from_week_ending
   ||!uuid.test(receipt.source_cycle_id)||!/^\d{4}-\d{2}-\d{2}$/.test(receipt.week_ending)
   ||receipt.week_ending<context.effective_from_week_ending
   ||(context.effective_to_week_ending!==null&&receipt.week_ending>context.effective_to_week_ending)
   ||receipt.effective_to_week_ending!==context.effective_to_week_ending||!uuid.test(receipt.scope_id)
   ||!hex.test(receipt.scope_sha256)||!hex.test(receipt.original_census_sha256)||!hex.test(receipt.source_cas_sha256)
   ||!/^[0-9]+$/.test(receipt.transaction_id))fail('PREREQUISITE_NATIVE_SCOPE_INVALID');
  validateExecution(receipt.execution,context);
  if(executionIdentity&&domainHash('EXECUTION_IDENTITY',executionIdentity)!==domainHash('EXECUTION_IDENTITY',receipt.execution))fail('PREREQUISITE_EXECUTION_MIXED');
  executionIdentity=receipt.execution;
  exact(receipt.assertions,prerequisiteGates[input.gate],'PREREQUISITE_ASSERTION_CENSUS_INVALID');
  if(Object.values(receipt.assertions).some(v=>v!==true))fail('PREREQUISITE_ASSERTION_FAILED');
  const native=receipt.native_execution_receipt;
  exact(native,['accepted','database','rolledBack','connectedFresh','tool_receipt_sha256','native_journey_id','scope_audit_rolled_back'],
   'PREREQUISITE_NATIVE_EXECUTION_INVALID');
  if(native.accepted!==true||native.database!==context.database||native.rolledBack!==true||native.connectedFresh!==true
   ||native.scope_audit_rolled_back!==true||!hex.test(native.tool_receipt_sha256)
   ||native.native_journey_id!=='PROTECTED_PREREQUISITE_'+input.gate)fail('PREREQUISITE_NATIVE_EXECUTION_INVALID');
  return {gate:input.gate,status:'PREREQUISITE_PASS',execution:'AGENCY_TEST_NATIVE_ROLLBACK',receipt_id:input.receipt_id,
   receipt_sha256:input.expected_receipt_sha256,execution_release_id:receipt.execution.release_id,
   execution_source_commit:receipt.execution.source_commit,execution_contract_sha256:receipt.execution.contract_sha256,
   execution_installation_id:receipt.execution.installation_id,scope_id:receipt.scope_id,scope_sha256:receipt.scope_sha256,
   transaction_id:receipt.transaction_id,source_cycle_id:receipt.source_cycle_id,week_ending:receipt.week_ending,
   original_census_sha256:receipt.original_census_sha256,source_cas_sha256:receipt.source_cas_sha256};
 }).sort((a,b)=>a.gate.localeCompare(b.gate));
 exact(sourceProof,['schema_version','execution_source_commit','installation_source_commit','inventory_schema','files','receipt_bindings','source_equivalence_sha256'],'PREREQUISITE_SOURCE_PROOF_INVALID');
 const {source_equivalence_sha256,...sourceBody}=sourceProof;
 if(sourceProof.schema_version!=='WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2'
  ||sourceProof.execution_source_commit!==executionIdentity.source_commit||sourceProof.installation_source_commit!==context.git_commit
  ||source_equivalence_sha256!==domainHash('WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2',sourceBody)
  ||!Array.isArray(sourceProof.files)||!sourceProof.files.length)fail('PREREQUISITE_SOURCE_PROOF_INVALID');
 const q={schema_version:'WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2',
  phase:'PREREQUISITES_QUALIFIED_OPERATIONAL_REPLAY_PENDING',release_id:context.release_id,git_commit:context.git_commit,
  contract_sha256:context.contract_sha256,agency_id:context.agency_id,environment:context.environment,source_group_id:context.source_group_id,
  effective_from_week_ending:context.effective_from_week_ending,effective_to_week_ending:context.effective_to_week_ending,
  capabilities:[...context.capabilities].sort(),execution_identity:executionIdentity,source_equivalence:sourceProof,
  gate_receipts:rows,required_operational_replay:[...requiredOperationalReplay]};
 return {...q,qualification_sha256:domainHash('WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2',q)};
}
export function assertManagedPrerequisiteEvidence(context,evidence,root){
 const rows=evidence?.weekly_source_protected_capability_qualifications;if(rows===undefined)return;
 if(!Array.isArray(rows)||rows.length>100)fail('PREREQUISITE_MANAGED_SCOPE_INVALID');
 const legacy=rows.filter(q=>q.schema_version==='WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1');
 assertLegacy(context,{weekly_source_protected_capability_qualifications:legacy});
 const seen=new Set();
 for(const q of rows.filter(q=>q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1')){
  exact(q,['schema_version','phase','release_id','git_commit','contract_sha256','agency_id','environment','source_group_id',
   'effective_from_week_ending','effective_to_week_ending','capabilities','execution_identity','source_equivalence','gate_receipts',
   'required_operational_replay','qualification_sha256'],'PREREQUISITE_MANAGED_RECEIPT_INVALID');
  const {qualification_sha256,...body}=q;
  validateQualificationContext({release_id:q.release_id,git_commit:q.git_commit,contract_sha256:q.contract_sha256,agency_id:q.agency_id,
   environment:q.environment,database:context.expectedDatabase,source_group_id:q.source_group_id,effective_from_week_ending:q.effective_from_week_ending,
   effective_to_week_ending:q.effective_to_week_ending,capabilities:q.capabilities});
  validateExecution(q.execution_identity,{...q,database:context.expectedDatabase});
  if(q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2'
   ||q.phase!=='PREREQUISITES_QUALIFIED_OPERATIONAL_REPLAY_PENDING'||q.release_id!==context.releaseId
   ||q.git_commit!==context.gitCommit||q.contract_sha256!==context.expectedHash
   ||domainHash(q.schema_version,body)!==qualification_sha256
   ||JSON.stringify(q.required_operational_replay)!==JSON.stringify(requiredOperationalReplay))fail('PREREQUISITE_MANAGED_RECEIPT_INVALID');
  const sourceProof=proveCommittedSources(root,q.execution_identity.source_commit,context.gitCommit);
  if(domainHash('SOURCE_EQUIVALENCE',q.source_equivalence)!==domainHash('SOURCE_EQUIVALENCE',sourceProof))fail('PREREQUISITE_NATIVE_SOURCE_CHANGED');
  const needed=Object.keys(prerequisiteGates).filter(g=>g!=='AUTOMATIC_COMPONENT_DELTA'||q.capabilities.includes('AUTOMATIC_RECONCILE'));
  if(!Array.isArray(q.gate_receipts)||q.gate_receipts.length!==needed.length||new Set(q.gate_receipts.map(g=>g.gate)).size!==needed.length)
   fail('PREREQUISITE_GATE_CENSUS_INVALID');
  for(const row of q.gate_receipts){
   exact(row,['gate','status','execution','receipt_id','receipt_sha256','execution_release_id','execution_source_commit',
    'execution_contract_sha256','execution_installation_id','scope_id','scope_sha256','transaction_id','source_cycle_id','week_ending','original_census_sha256','source_cas_sha256'],
    'PREREQUISITE_MANAGED_GATE_INVALID');
   if(!needed.includes(row.gate)||row.status!=='PREREQUISITE_PASS'||row.execution!=='AGENCY_TEST_NATIVE_ROLLBACK'
    ||row.execution_release_id!==q.execution_identity.release_id||row.execution_source_commit!==q.execution_identity.source_commit
    ||row.execution_contract_sha256!==q.execution_identity.contract_sha256||row.execution_installation_id!==q.execution_identity.installation_id
    ||!hex.test(row.receipt_sha256)||!hex.test(row.scope_sha256)||!hex.test(row.original_census_sha256)||!hex.test(row.source_cas_sha256)
    ||!uuid.test(row.scope_id)||!uuid.test(row.source_cycle_id)||!/^[0-9]+$/.test(row.transaction_id)
    ||!/^\d{4}-\d{2}-\d{2}$/.test(row.week_ending)||row.week_ending<q.effective_from_week_ending
    ||(q.effective_to_week_ending!==null&&row.week_ending>q.effective_to_week_ending))fail('PREREQUISITE_MANAGED_GATE_INVALID');
  }
  const key=q.source_group_id+':'+q.effective_from_week_ending+':'+q.effective_to_week_ending;
  if(seen.has(key))fail('PREREQUISITE_MANAGED_DUPLICATE');seen.add(key);
 }
}
