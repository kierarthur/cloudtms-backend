import crypto from 'node:crypto';
import {buildQualification,validateQualificationContext,domainHash,exact,assertManagedQualificationEvidence} from './weekly-source-protected-release-qualification.mjs';
import {proveCommittedSources,loadCommittedBundle,assertExecutionTreeProof} from './weekly-source-protected-execution-tree.mjs';
import {assertActivationWindowBinding} from './weekly-source-activation-window-binding.mjs';
const fail=code=>{throw Error(code);},sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const hex=/^[0-9a-f]{64}$/,uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const validDate=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)
 &&Number.isFinite(Date.parse(value+'T00:00:00Z'))&&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value;
const validTimestamp=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
 &&Number.isFinite(Date.parse(value));
export const operationalSchema='WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1';
export const remoteGates=Object.freeze({
 CLIENT_HTTP:{origin:'AUTHENTICATED_DEPLOYED_HTTP',assertions:['real_authenticated_session','trusted_actor_injected','closed_browser_payload',
  'native_preview_commit_retry','native_source_cas_error_order','foreign_actor_and_scope_refused','operational_readiness_false']},
 OFFICE_UI:{origin:'DEPLOYED_OFFICE_BROWSER_UI',assertions:['actual_deployed_bundle','deep_link_scope','actual_preview_and_confirmation',
  'native_partial_retry_display','unauthorised_controls_absent','operational_readiness_false']},
 MYTMS_PHONE:{origin:'PHYSICAL_TESTFLIGHT_MYTMS_PHONE',assertions:['actual_installed_test_build','actual_candidate_authenticated_session',
  'genuine_signed_submission_bytes','native_signature_authority','actual_approved_hours_display','actual_document_bytes_and_hash',
  'operational_readiness_false']}
});
// These are provenance checks, not proof that a journey happened. The protected
// collector must derive them from actual remote captures/native audit receipts.
// Unit vectors can never be used as a committed acceptance input.
export function buildOperationalQualification(context,nativeContext,nativeInputs,remoteInputs,setupClosure,sourceProof,activationScope){
 validateQualificationContext(context);validateQualificationContext(nativeContext);
 if((nativeContext.release_id===context.release_id&&nativeContext.git_commit!==context.git_commit)||nativeContext.contract_sha256!==context.contract_sha256
  ||['agency_id','environment','database','source_group_id'].some(k=>nativeContext[k]!==context[k])
  ||JSON.stringify([...nativeContext.capabilities].sort())!==JSON.stringify([...context.capabilities].sort()))fail('OPERATIONAL_EXECUTION_SCOPE_INVALID');
 const native=buildQualification(nativeContext,nativeInputs); // Original 9/10 gates retained; no aliases for CLIENT_API/DOCUMENT.
 assertExecutionTreeProof(sourceProof,nativeContext.git_commit,sourceProof.installation_source_commit);
 exact(sourceProof,['schema_version','execution_source_commit','installation_source_commit','inventory_schema','files','receipt_bindings','source_equivalence_sha256'],
  'OPERATIONAL_SOURCE_PROOF_INVALID');
 const {source_equivalence_sha256,...sourceBody}=sourceProof;
 if(sourceProof.schema_version!=='WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2'
  ||sourceProof.execution_source_commit!==nativeContext.git_commit||!/^[0-9a-f]{40}$/.test(sourceProof.installation_source_commit)
  ||domainHash(sourceProof.schema_version,sourceBody)!==source_equivalence_sha256)fail('OPERATIONAL_SOURCE_PROOF_INVALID');
 exact(setupClosure,['scope_id','scope_sha256','setup_authorization_sha256','original_census_sha256','actor_user_id','source_cycle_id',
  'week_ending','execution_release_id','execution_source_commit','execution_contract_sha256','closure_audit_id','closure_audit_sha256',
  'state','closed_at_utc','all_assignments_disabled','all_operational_producers_false'],'OPERATIONAL_SETUP_CLOSURE_INVALID');
 if(!uuid.test(setupClosure.scope_id)||!uuid.test(setupClosure.actor_user_id)||!uuid.test(setupClosure.source_cycle_id)
  ||!uuid.test(setupClosure.closure_audit_id)||setupClosure.state!=='CLOSED'
  ||setupClosure.all_assignments_disabled!==true||setupClosure.all_operational_producers_false!==true
  ||setupClosure.execution_release_id!==nativeContext.release_id||setupClosure.execution_source_commit!==nativeContext.git_commit
  ||setupClosure.execution_contract_sha256!==nativeContext.contract_sha256
  ||!validDate(setupClosure.week_ending)||setupClosure.week_ending<nativeContext.effective_from_week_ending
  ||(nativeContext.effective_to_week_ending!==null&&setupClosure.week_ending>nativeContext.effective_to_week_ending)
  ||!validTimestamp(setupClosure.closed_at_utc)
  ||['scope_sha256','setup_authorization_sha256','original_census_sha256','closure_audit_sha256'].some(k=>!hex.test(setupClosure[k])))
  fail('OPERATIONAL_SETUP_CLOSURE_INVALID');
 if(!Array.isArray(remoteInputs)||remoteInputs.length!==Object.keys(remoteGates).length
  ||new Set(remoteInputs.map(r=>r.gate)).size!==Object.keys(remoteGates).length)fail('OPERATIONAL_REMOTE_GATE_CENSUS_INVALID');
 const remote=remoteInputs.map(input=>{
  exact(input,['gate','receipt_id','receipt_bytes','expected_receipt_sha256'],'OPERATIONAL_REMOTE_INPUT_INVALID');
  const spec=remoteGates[input.gate];
  if(!spec||typeof input.receipt_id!=='string'||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(input.receipt_id)
   ||!(Buffer.isBuffer(input.receipt_bytes)||typeof input.receipt_bytes==='string')||sha(input.receipt_bytes)!==input.expected_receipt_sha256)
   fail('OPERATIONAL_REMOTE_ARTIFACT_INVALID');
  const row=JSON.parse(input.receipt_bytes.toString('utf8'));
  exact(row,['schema_version','gate','origin','execution_release_id','execution_source_commit','execution_contract_sha256','environment','database',
   'agency_id','source_group_id','source_cycle_id','actor_user_id','week_ending','scope_id','scope_sha256','original_census_sha256',
   'server_request_ids','server_trace_sha256','native_result_receipts_sha256','capture_sha256','deployments','assertions'],'OPERATIONAL_REMOTE_RECEIPT_INVALID');
  if(row.schema_version!=='WEEKLY_SOURCE_PROTECTED_REMOTE_GATE_RECEIPT_V1'||row.gate!==input.gate||row.origin!==spec.origin
   ||row.execution_release_id!==nativeContext.release_id||row.execution_source_commit!==nativeContext.git_commit
   ||row.execution_contract_sha256!==nativeContext.contract_sha256||row.environment!=='TEST'||row.database!==context.database
   ||row.agency_id!==context.agency_id||row.source_group_id!==context.source_group_id
   ||['source_cycle_id','actor_user_id','week_ending','scope_id','scope_sha256','original_census_sha256'].some(k=>row[k]!==setupClosure[k])
   ||!Array.isArray(row.server_request_ids)||!row.server_request_ids.length||row.server_request_ids.length>100
   ||row.server_request_ids.some(id=>typeof id!=='string'||!/^[A-Za-z0-9:_-]{1,128}$/.test(id))
   ||new Set(row.server_request_ids).size!==row.server_request_ids.length
   ||['server_trace_sha256','native_result_receipts_sha256','capture_sha256'].some(k=>!hex.test(row[k])))fail('OPERATIONAL_REMOTE_SCOPE_INVALID');
  const deployments=row.deployments;
  exact(deployments,['worker_script','worker_version_id','worker_source_commit','office_source_commit','office_bundle_sha256',
   'mytms_source_commit','mytms_build_id','mytms_bundle_identifier','mytms_build_artifact_sha256'],'OPERATIONAL_DEPLOYMENT_INVALID');
  if(deployments.worker_script!=='test-cloudtms-backend'||!uuid.test(deployments.worker_version_id)
   ||deployments.worker_source_commit!==nativeContext.git_commit
   ||!['office_source_commit','mytms_source_commit'].every(k=>/^[0-9a-f]{40}$/.test(deployments[k]))
   ||!hex.test(deployments.office_bundle_sha256)||!hex.test(deployments.mytms_build_artifact_sha256)
   ||typeof deployments.mytms_build_id!=='string'||!deployments.mytms_build_id
   ||deployments.mytms_bundle_identifier!=='net.cloudtms.mytms.test')fail('OPERATIONAL_DEPLOYMENT_INVALID');
  exact(row.assertions,spec.assertions,'OPERATIONAL_REMOTE_ASSERTION_CENSUS_INVALID');
  if(Object.values(row.assertions).some(v=>v!==true))fail('OPERATIONAL_REMOTE_ASSERTION_FAILED');
  return {...row,receipt_id:input.receipt_id,receipt_sha256:input.expected_receipt_sha256};
 }).sort((a,b)=>a.gate.localeCompare(b.gate));
 if(new Set(remote.map(r=>domainHash('DEPLOYED_REMOTE_IDENTITIES',r.deployments))).size!==1)fail('OPERATIONAL_DEPLOYMENT_MIXED');
 assertActivationWindowBinding(activationScope,context,nativeContext,setupClosure);
 const body={activation_scope:activationScope,receipt_input_commit:sourceProof.installation_source_commit,schema_version:operationalSchema,phase:'ALL_PRE_SWITCH_GATES_QUALIFIED',release_id:context.release_id,
  git_commit:context.git_commit,contract_sha256:context.contract_sha256,agency_id:context.agency_id,environment:context.environment,
  source_group_id:context.source_group_id,effective_from_week_ending:context.effective_from_week_ending,effective_to_week_ending:context.effective_to_week_ending,
  capabilities:[...context.capabilities].sort(),native_execution_qualification:native,remote_gate_receipts:remote,
  test_setup_closure:setupClosure,source_equivalence:sourceProof};
 return {...body,qualification_sha256:domainHash(operationalSchema,body)};
}
export function operationalCertificate(value){
 return value?.schema_version===operationalSchema&&value.phase==='ALL_PRE_SWITCH_GATES_QUALIFIED'
  &&value.remote_gate_receipts?.length===3&&value.test_setup_closure?.state==='CLOSED';
}
export function requireOperationalCertificate(value){if(!operationalCertificate(value))fail('WEEKLY_SOURCE_PROTECTED_OPERATIONAL_GATES_REQUIRED');return value;}
// Protocol view of the real chain; source regressions also pin the SQL hunks.
export function activationReceiptReady(value,assignment){
 try{requireOperationalCertificate(value);return assignment.feature_enabled===true
  &&JSON.stringify(assignment.protected_capability_qualification)===JSON.stringify(value);}catch{return false;}
}
export function loadOperationalJourneys(root,release,context){
 const receiptInputCommit=context.receiptInputCommit??context.gitCommit;
 const bundle=loadCommittedBundle(root,release,{...context,gitCommit:receiptInputCommit},'weeklySourceProtectedOperational');if(bundle===null)return [];
 if(!/^[0-9a-f]{40}$/.test(receiptInputCommit))fail('OPERATIONAL_RECEIPT_INPUT_COMMIT_INVALID');
 exact(bundle,['schema_version','qualifications',...['report_fin_qualifications','report_fin_native_inputs','report_fin_native_artifacts']
  .filter(key=>Object.hasOwn(bundle,key))],'OPERATIONAL_BUNDLE_INVALID');
 if(bundle.schema_version!=='WEEKLY_SOURCE_PROTECTED_OPERATIONAL_INPUT_BUNDLE_V1'||!Array.isArray(bundle.qualifications)
  ||bundle.qualifications.length>100)fail('OPERATIONAL_BUNDLE_INVALID');
 return bundle.qualifications.map(row=>{
  exact(row,['context','native_context','native_gate_inputs','remote_gate_inputs','setup_closure','activation_scope'],'OPERATIONAL_BUNDLE_SCOPE_INVALID');
  const installation=row.context;
  if(installation.release_id!==context.releaseId||installation.git_commit!==context.gitCommit
   ||installation.contract_sha256!==context.expectedHash||installation.environment!==context.environment
   ||installation.database!==context.expectedDatabase)fail('OPERATIONAL_BUNDLE_EXECUTION_RELABELLED');
  const unpack=rows=>rows.map(input=>{
   exact(input,['gate','receipt_id','receipt','receipt_sha256'],'OPERATIONAL_BUNDLE_GATE_INVALID');
   if(typeof input.receipt!=='string'||sha(input.receipt)!==input.receipt_sha256)fail('OPERATIONAL_REMOTE_ARTIFACT_INVALID');
   return {gate:input.gate,receipt_id:input.receipt_id,receipt_bytes:input.receipt,expected_receipt_sha256:input.receipt_sha256};
  });
  const source=proveCommittedSources(root,row.native_context.git_commit,receiptInputCommit);
  const qualification=buildOperationalQualification(installation,row.native_context,unpack(row.native_gate_inputs),unpack(row.remote_gate_inputs),row.setup_closure,source,row.activation_scope);
  return {phase:'FULL_OPERATIONAL',context:installation,qualification};
 });
}
export function assertManagedOperationalEvidence(context,evidence,root){
 const rows=evidence?.weekly_source_protected_capability_qualifications??[];
 if(!Array.isArray(rows)||rows.length>100)fail('OPERATIONAL_MANAGED_SCOPE_INVALID');
 const seen=new Set();
 for(const q of rows){
  exact(q,['schema_version','phase','release_id','git_commit','contract_sha256','agency_id','environment','source_group_id',
   'effective_from_week_ending','effective_to_week_ending','capabilities','native_execution_qualification','remote_gate_receipts',
   'test_setup_closure','source_equivalence','activation_scope','receipt_input_commit','qualification_sha256'],'OPERATIONAL_MANAGED_CERTIFICATE_INVALID');
  requireOperationalCertificate(q);
  const {qualification_sha256,...body}=q;
  if(q.release_id!==context.releaseId||q.git_commit!==context.gitCommit||q.contract_sha256!==context.expectedHash
   ||q.environment!=='TEST'||context.expectedDatabase!=='cloudtms_test_clone'||domainHash(operationalSchema,body)!==qualification_sha256)
   fail('OPERATIONAL_MANAGED_CERTIFICATE_INVALID');
  const native=q.native_execution_qualification;
  assertManagedQualificationEvidence({environment:'TEST',expectedDatabase:context.expectedDatabase,releaseId:native.release_id,
   gitCommit:native.git_commit,expectedHash:native.contract_sha256},{weekly_source_protected_capability_qualifications:[native]});
  if((native.release_id===q.release_id&&native.git_commit!==q.git_commit)||native.contract_sha256!==q.contract_sha256
   ||['agency_id','environment','source_group_id'].some(k=>native[k]!==q[k])
   ||JSON.stringify(native.capabilities)!==JSON.stringify(q.capabilities))fail('OPERATIONAL_EXECUTION_SCOPE_INVALID');
  const closure=q.test_setup_closure;
  exact(closure,['scope_id','scope_sha256','setup_authorization_sha256','original_census_sha256','actor_user_id','source_cycle_id',
   'week_ending','execution_release_id','execution_source_commit','execution_contract_sha256','closure_audit_id','closure_audit_sha256',
   'state','closed_at_utc','all_assignments_disabled','all_operational_producers_false'],'OPERATIONAL_SETUP_CLOSURE_INVALID');
  if(!['scope_id','actor_user_id','source_cycle_id','closure_audit_id'].every(k=>uuid.test(closure[k]))
   ||!['scope_sha256','setup_authorization_sha256','original_census_sha256','closure_audit_sha256'].every(k=>hex.test(closure[k]))
   ||!validTimestamp(closure.closed_at_utc)||!validDate(closure.week_ending)||closure.week_ending<native.effective_from_week_ending
   ||(native.effective_to_week_ending!==null&&closure.week_ending>native.effective_to_week_ending)
   ||closure.state!=='CLOSED'||closure.all_assignments_disabled!==true||closure.all_operational_producers_false!==true
   ||closure.execution_release_id!==native.release_id||closure.execution_source_commit!==native.git_commit
   ||closure.execution_contract_sha256!==native.contract_sha256)fail('OPERATIONAL_SETUP_CLOSURE_INVALID');
  if(new Set(q.remote_gate_receipts.map(row=>row.gate)).size!==3)fail('OPERATIONAL_REMOTE_GATE_CENSUS_INVALID');
  for(const row of q.remote_gate_receipts){
   const spec=remoteGates[row.gate];
   exact(row,['schema_version','gate','origin','execution_release_id','execution_source_commit','execution_contract_sha256','environment','database',
    'agency_id','source_group_id','source_cycle_id','actor_user_id','week_ending','scope_id','scope_sha256','original_census_sha256',
    'server_request_ids','server_trace_sha256','native_result_receipts_sha256','capture_sha256','deployments','assertions','receipt_id','receipt_sha256'],
    'OPERATIONAL_REMOTE_RECEIPT_INVALID');
   if(!spec||row.origin!==spec.origin||row.schema_version!=='WEEKLY_SOURCE_PROTECTED_REMOTE_GATE_RECEIPT_V1'
    ||row.execution_release_id!==native.release_id||row.execution_source_commit!==native.git_commit
    ||row.execution_contract_sha256!==native.contract_sha256||row.environment!=='TEST'||row.database!==context.expectedDatabase
    ||row.agency_id!==q.agency_id||row.source_group_id!==q.source_group_id
    ||['source_cycle_id','actor_user_id','week_ending','scope_id','scope_sha256','original_census_sha256'].some(k=>row[k]!==closure[k])
    ||typeof row.receipt_id!=='string'||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(row.receipt_id)
    ||['receipt_sha256','server_trace_sha256','native_result_receipts_sha256','capture_sha256'].some(k=>!hex.test(row[k])))
    fail('OPERATIONAL_REMOTE_SCOPE_INVALID');
   exact(row.assertions,spec.assertions,'OPERATIONAL_REMOTE_ASSERTION_CENSUS_INVALID');
   if(Object.values(row.assertions).some(v=>v!==true))fail('OPERATIONAL_REMOTE_ASSERTION_FAILED');
   exact(row.deployments,['worker_script','worker_version_id','worker_source_commit','office_source_commit','office_bundle_sha256',
    'mytms_source_commit','mytms_build_id','mytms_bundle_identifier','mytms_build_artifact_sha256'],'OPERATIONAL_DEPLOYMENT_INVALID');
   if(!Array.isArray(row.server_request_ids)||!row.server_request_ids.length||row.server_request_ids.length>100
    ||row.server_request_ids.some(id=>typeof id!=='string'||!/^[A-Za-z0-9:_-]{1,128}$/.test(id))
    ||new Set(row.server_request_ids).size!==row.server_request_ids.length
    ||row.deployments.worker_script!=='test-cloudtms-backend'||row.deployments.worker_source_commit!==native.git_commit
    ||!uuid.test(row.deployments.worker_version_id)
    ||!['office_source_commit','mytms_source_commit'].every(k=>/^[0-9a-f]{40}$/.test(row.deployments[k]))
    ||!['office_bundle_sha256','mytms_build_artifact_sha256'].every(k=>hex.test(row.deployments[k]))
    ||typeof row.deployments.mytms_build_id!=='string'||!row.deployments.mytms_build_id
    ||row.deployments.mytms_bundle_identifier!=='net.cloudtms.mytms.test')fail('OPERATIONAL_DEPLOYMENT_INVALID');
  }
  if(new Set(q.remote_gate_receipts.map(row=>domainHash('DEPLOYED_REMOTE_IDENTITIES',row.deployments))).size!==1)fail('OPERATIONAL_DEPLOYMENT_MIXED');
  assertActivationWindowBinding(q.activation_scope,q,native,closure);
  if(!/^[0-9a-f]{40}$/.test(q.receipt_input_commit))fail('OPERATIONAL_RECEIPT_INPUT_COMMIT_INVALID');
  const proof=proveCommittedSources(root,native.git_commit,q.receipt_input_commit);
  if(domainHash('EXACT_EXECUTION_TREE_PROOF',proof)!==domainHash('EXACT_EXECUTION_TREE_PROOF',q.source_equivalence))
   fail('OPERATIONAL_NATIVE_SOURCE_CHANGED');
  const key=[q.source_group_id,q.effective_from_week_ending,q.effective_to_week_ending].join(':');
  if(seen.has(key))fail('OPERATIONAL_DUPLICATE_SCOPE');seen.add(key);
 }
}
