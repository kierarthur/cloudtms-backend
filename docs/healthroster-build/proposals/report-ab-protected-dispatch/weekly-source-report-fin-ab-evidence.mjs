import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {exact,domainHash,sha256} from './weekly-source-protected-release-qualification.mjs';
import {proveCommittedSources,assertExecutionTreeProof,loadCommittedBundle,sourceInventory} from './weekly-source-protected-execution-tree.mjs';

const fail=code=>{throw Error(code);},hex=/^[0-9a-f]{64}$/,commit=/^[0-9a-f]{40}$/,
 uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export const schema='WEEKLY_SOURCE_REPORT_FIN_AB_QUALIFICATION_V1';
export const caseIds=Object.freeze(['BOUND_NEGATIVE','D_NEGATIVE','D_POSITIVE','D_ZERO','NEXT_ACTIVE','ORDINARY_NHSP_REGRESSION','STANDALONE_NEGATIVE']);
export const assertions=Object.freeze(['actual_report_and_export_binding','bound_export_pay_finalised','native_fin_atomic',
 'native_invoice_admission','native_invoice_validation','native_frozen_render','invoice_target_matches_original',
 'rounding_identity_and_last_line_checked','zero_invoice_only_pay_vectors','no_report_pay_orchestrator',
 'no_row_resolution_pay_entitlement',
 'native_candidate_i1_hooks_retained','no_provider_settlement_email_network','rollback_fresh_restored']);
export const assertionsForCase=id=>[...assertions,...(id==='NEXT_ACTIVE'?['next_active_no_expense_observation']:[]),
 ...(id==='ORDINARY_NHSP_REGRESSION'?['ordinary_nhsp_unchanged']:[])];
export const executionKeys=Object.freeze(['release_id','source_commit','contract_sha256','installation_id','environment','database',
 'managed_manifest_sha256','installed_catalogue_jsonb_sha256']);
const scopeKeys=['agency_id','source_group_id','rule_set_version_id','rule_set_values_sha256','effective_from_week_ending',
 'effective_to_week_ending'];
const date=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)&&Number.isFinite(Date.parse(value+'T00:00:00Z'))
 &&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value;
function scope(value){
 exact(value,scopeKeys,'REPORT_AB_SCOPE_INVALID');
 if(!['agency_id','source_group_id','rule_set_version_id'].every(k=>uuid.test(value[k]))||!hex.test(value.rule_set_values_sha256)
  ||!date(value.effective_from_week_ending)||(value.effective_to_week_ending!==null&&(!date(value.effective_to_week_ending)
  ||value.effective_to_week_ending<value.effective_from_week_ending)))fail('REPORT_AB_SCOPE_INVALID');
}
function execution(value){
 exact(value,executionKeys,'REPORT_AB_EXECUTION_INVALID');
 if(value.environment!=='TEST'||value.database!=='cloudtms_test_clone'||!value.release_id||!commit.test(value.source_commit)
  ||!uuid.test(value.installation_id)||!['contract_sha256','managed_manifest_sha256','installed_catalogue_jsonb_sha256'].every(k=>hex.test(value[k])))
  fail('REPORT_AB_EXECUTION_INVALID');
}
// These validators do not establish that a native journey happened. Only the
// root's real rollback collector may supply these byte-preserved envelopes.
// Unit-test envelopes MUST NEVER be retained as release evidence.
export function buildNativeComparisons(actualExecution,inputs,{complete=false}={}){
 execution(actualExecution);
 if(!Array.isArray(inputs)||!inputs.length||inputs.length>caseIds.length*2||(complete&&inputs.length!==caseIds.length*2))fail('REPORT_AB_NATIVE_CASE_CENSUS_INVALID');
 const rows=inputs.map(input=>{
  exact(input,['receipt_id','receipt_bytes','receipt_sha256'],'REPORT_AB_NATIVE_INPUT_INVALID');
  if(typeof input.receipt_id!=='string'||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(input.receipt_id)
   ||!(typeof input.receipt_bytes==='string'||Buffer.isBuffer(input.receipt_bytes))||sha256(input.receipt_bytes)!==input.receipt_sha256)
   fail('REPORT_AB_NATIVE_ARTIFACT_INVALID');
  const row=JSON.parse(input.receipt_bytes.toString('utf8'));
  exact(row,['schema_version','execution','scope','case_id','variant','source_fixture_sha256','original_report_sha256',
   'report_row_manifest_sha256','binding_authority_sha256','native_tool_receipt','native_result','comparison','assertions'],
   'REPORT_AB_NATIVE_RECEIPT_INVALID');
  if(row.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_VARIANT_V1'||!caseIds.includes(row.case_id)
   ||!['A','B'].includes(row.variant))fail('REPORT_AB_NATIVE_RECEIPT_INVALID');
  execution(row.execution);scope(row.scope);
  if(domainHash('REPORT_AB_EXECUTION',row.execution)!==domainHash('REPORT_AB_EXECUTION',actualExecution))fail('REPORT_AB_NATIVE_EXECUTION_CHANGED');
  for(const k of ['source_fixture_sha256','original_report_sha256','report_row_manifest_sha256','binding_authority_sha256'])
   if(!hex.test(row[k]))fail('REPORT_AB_NATIVE_RECEIPT_INVALID');
  exact(row.native_tool_receipt,['accepted','database','rolledBack','connectedFresh','tool_receipt_sha256','journey_id'],'REPORT_AB_TOOL_RECEIPT_INVALID');
  if(row.native_tool_receipt.accepted!==true||row.native_tool_receipt.database!==actualExecution.database
   ||row.native_tool_receipt.rolledBack!==true||row.native_tool_receipt.connectedFresh!==true
   ||!hex.test(row.native_tool_receipt.tool_receipt_sha256)
   ||row.native_tool_receipt.journey_id!=='REPORT_FIN_'+row.case_id+'_'+row.variant)fail('REPORT_AB_TOOL_RECEIPT_INVALID');
  exact(row.native_result,['source_cycle_id','report_scope_id','final_revision_id','client_manifest_id','invoice_id',
   'fin_result_sha256','admission_receipt_sha256','validation_receipt_sha256','rendered_document_sha256','pay_isolation_receipt_sha256'],
   'REPORT_AB_NATIVE_RESULT_INVALID');
  if(!['source_cycle_id','report_scope_id','final_revision_id','client_manifest_id','invoice_id'].every(k=>uuid.test(row.native_result[k]))
   ||!Object.keys(row.native_result).filter(k=>k.endsWith('_sha256')).every(k=>hex.test(row.native_result[k])))fail('REPORT_AB_NATIVE_RESULT_INVALID');
  exact(row.comparison,['schema_version','net_pence','vat_pence','gross_pence','rounding_pence','report_lines_sha256',
   'invoice_target_sha256','pay_isolation_sha256','render_semantics_sha256'],'REPORT_AB_COMPARISON_INVALID');
  if(row.comparison.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_COMPARISON_V1'
   ||!['net_pence','vat_pence','gross_pence','rounding_pence'].every(k=>typeof row.comparison[k]==='string'&&/^(0|-?[1-9][0-9]{0,14})$/.test(row.comparison[k]))
   ||!Object.keys(row.comparison).filter(k=>k.endsWith('_sha256')).every(k=>hex.test(row.comparison[k])))fail('REPORT_AB_COMPARISON_INVALID');
  if(BigInt(row.comparison.net_pence)+BigInt(row.comparison.vat_pence)!==BigInt(row.comparison.gross_pence)
   ||(row.case_id==='D_POSITIVE'&&BigInt(row.comparison.rounding_pence)<=0n)
   ||(row.case_id==='D_NEGATIVE'&&BigInt(row.comparison.rounding_pence)>=0n)
   ||(row.case_id==='D_ZERO'&&BigInt(row.comparison.rounding_pence)!==0n))fail('REPORT_AB_NATIVE_COMPARISON_CASE_INVALID');
  exact(row.assertions,assertionsForCase(row.case_id),'REPORT_AB_ASSERTION_CENSUS_INVALID');
  if(Object.values(row.assertions).some(v=>v!==true))fail('REPORT_AB_NATIVE_ASSERTION_FAILED');
  const body={...row,receipt_id:input.receipt_id,receipt_sha256:input.receipt_sha256};
  return {...body,comparison_sha256:domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_VARIANT_RECORD_V1',body)};
 }).sort((a,b)=>a.case_id.localeCompare(b.case_id)||a.variant.localeCompare(b.variant));
 if(new Set(rows.map(r=>r.case_id+':'+r.variant)).size!==inputs.length)fail('REPORT_AB_NATIVE_CASE_CENSUS_INVALID');
 for(const id of caseIds){
  const a=rows.find(r=>r.case_id===id&&r.variant==='A'),b=rows.find(r=>r.case_id===id&&r.variant==='B');
  if(!a||!b){if(complete)fail('REPORT_AB_NATIVE_CASE_CENSUS_INVALID');continue;}
  if(['scope','comparison'].some(k=>domainHash('REPORT_AB_'+k,a[k])!==domainHash('REPORT_AB_'+k,b[k]))
   ||['source_fixture_sha256','original_report_sha256','report_row_manifest_sha256','binding_authority_sha256'].some(k=>a[k]!==b[k])
   ||a.native_result.source_cycle_id!==b.native_result.source_cycle_id||a.native_result.report_scope_id!==b.native_result.report_scope_id)
   fail('REPORT_AB_VARIANTS_NOT_SAME_NATIVE_BASIS');
 }
 if(new Set(rows.map(r=>domainHash('REPORT_AB_SCOPE',r.scope))).size!==1)fail('REPORT_AB_NATIVE_SCOPE_MIXED');
 return rows;
}
export function assertNativeComparisons(actualExecution,rows,{complete=false}={}){
 const inputs=rows.map(row=>{const {receipt_id,receipt_sha256,comparison_sha256,...body}=row;
  // The original raw receipt hash is retained separately; its exact bytes are
  // checked at capture. Reconstructing JSON would not reproduce its bytes.
  if(!hex.test(receipt_sha256)||domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_VARIANT_RECORD_V1',{...body,receipt_id,receipt_sha256})!==comparison_sha256)
   fail('REPORT_AB_NATIVE_ARTIFACT_INVALID');
  return {receipt_id,receipt_bytes:JSON.stringify(body),receipt_sha256:sha256(JSON.stringify(body))};});
 const rebuilt=buildNativeComparisons(actualExecution,inputs,{complete});
 if(rebuilt.some((row,i)=>{const {comparison_sha256,...body}=row;
  const retainedBody={...body,receipt_sha256:rows[i].receipt_sha256};
  return domainHash('REPORT_AB_RETAINED_BODY',{...retainedBody,comparison_sha256:domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_VARIANT_RECORD_V1',retainedBody)})!==domainHash('REPORT_AB_RETAINED_BODY',rows[i]);}))
  fail('REPORT_AB_RETAINED_BODY_CHANGED');
 return rows;
}
function decision(value,rows){
 exact(value,['schema_version','selected_variant','native_comparison_sha256','reason'],'REPORT_AB_DECISION_INVALID');
 if(value.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_REVIEWED_DECISION_V1'||!['A','B'].includes(value.selected_variant)
  ||value.native_comparison_sha256!==domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_COMPARISONS_V1',rows)
  ||typeof value.reason!=='string'||value.reason.trim()!==value.reason||value.reason.length<1||value.reason.length>1000)fail('REPORT_AB_DECISION_INVALID');
}
export function buildQualification(installation,recordedExecution,rows,reviewedDecision,decisionBinding,sourceProof,receiptInputCommit){
 execution(installation);execution(recordedExecution);assertNativeComparisons(recordedExecution,rows,{complete:true});decision(reviewedDecision,rows);
 if(domainHash('REPORT_AB_INSTALLED_EXECUTION',installation)!==domainHash('REPORT_AB_INSTALLED_EXECUTION',recordedExecution)
  ||!commit.test(receiptInputCommit))fail('REPORT_AB_INSTALLED_EXECUTION_CHANGED');
 exact(decisionBinding,['path','blob_oid','sha256','json_pointer'],'REPORT_AB_DECISION_BINDING_INVALID');
 if(decisionBinding.path!=='supabase/release/weekly-source-protected-operational.json'
  ||!/^\/report_fin_qualifications\/(0|[1-9][0-9]?)\/decision$/.test(decisionBinding.json_pointer)
  ||!commit.test(decisionBinding.blob_oid)||!hex.test(decisionBinding.sha256))fail('REPORT_AB_DECISION_BINDING_INVALID');
 assertExecutionTreeProof(sourceProof,recordedExecution.source_commit,receiptInputCommit);
 if(!sourceProof.receipt_bindings.some(row=>row.role==='INSTALLATION'&&row.key==='weeklySourceProtectedOperational'
  &&row.path===decisionBinding.path&&row.blob_oid===decisionBinding.blob_oid&&row.sha256===decisionBinding.sha256))
  fail('REPORT_AB_DECISION_NOT_IN_EXECUTION_PROOF');
 const body={schema_version:schema,phase:'BOTH_NATIVE_VARIANTS_QUALIFIED',installation,receipt_input_commit:receiptInputCommit,scope:rows[0].scope,
  execution:recordedExecution,native_comparisons:rows,decision:reviewedDecision,decision_binding:decisionBinding,source_equivalence:sourceProof};
 return {...body,qualification_sha256:domainHash(schema,body)};
}
const sqlJson=(value,literal)=>literal(JSON.stringify(value))+'::jsonb';
export function loadReportFinInputs(root,release,context){
 if(!commit.test(context.receiptInputCommit))fail('REPORT_AB_RECEIPT_INPUT_COMMIT_REQUIRED');
 // Installed source remains context.gitCommit. This read explicitly binds the
 // separate receipt-data commit, never silently relabels installed execution.
 const bundle=loadCommittedBundle(root,release,{...context,gitCommit:context.receiptInputCommit},'weeklySourceProtectedOperational');
 if(bundle===null||bundle.report_fin_qualifications===undefined)return [];
 exact(bundle,['schema_version','qualifications','report_fin_qualifications',
  ...(bundle.report_fin_native_inputs===undefined?[]:['report_fin_native_inputs'])],'REPORT_AB_COMMITTED_BUNDLE_INVALID');
 if(bundle.schema_version!=='WEEKLY_SOURCE_PROTECTED_OPERATIONAL_INPUT_BUNDLE_V1'||!Array.isArray(bundle.qualifications)
  ||!Array.isArray(bundle.report_fin_qualifications)||!bundle.report_fin_qualifications.length||bundle.report_fin_qualifications.length>100)
  fail('REPORT_AB_COMMITTED_BUNDLE_INVALID');
 const file='supabase/release/weekly-source-protected-operational.json',descriptor=release.weeklySourceProtectedOperational;
 const blob=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','rev-parse',context.receiptInputCommit+':'+file],
  {cwd:root,encoding:'utf8'}).trim();
 return bundle.report_fin_qualifications.map((input,index)=>{
  exact(input,['execution_release_id','decision'],'REPORT_AB_COMMITTED_INPUT_INVALID');
  return {...input,decision_binding:{path:file,blob_oid:blob,sha256:descriptor.sha256,json_pointer:'/report_fin_qualifications/'+index+'/decision'}};
 });
}
export function recordedReleaseSql(releaseId,literal){
 if(typeof releaseId!=='string'||!/^[A-Za-z0-9_:@.-]{1,256}$/.test(releaseId))fail('REPORT_AB_RELEASE_ID_INVALID');
 return 'select jsonb_build_object('+literal('execution')+',jsonb_build_object('+executionKeys.map(k=>literal(k)+','+({release_id:'r.release_id',
  source_commit:'r.git_commit',contract_sha256:'r.installed_contract_sha256',installation_id:'i.installation_id',environment:'i.environment',
  database:'current_database()',managed_manifest_sha256:"r.evidence_json#>>'{managed_release,manifest_sha256}'",
  installed_catalogue_jsonb_sha256:"r.evidence_json#>>'{weekly_source_protected_qualification_catalogue,jsonb_sha256}'"})[k]).join(',')+'),'+
  literal('native_comparisons')+",r.evidence_json->'weekly_source_report_fin_native_comparisons')::text from private.cloudtms_database_releases r "+
  "cross join private.cloudtms_database_identity i where i.singleton and i.environment='TEST' and r.release_id="+literal(releaseId)+
  " and r.status='VERIFIED' and r.completed_at_utc is not null and r.installed_contract_sha256=r.repository_contract_sha256;";
}
export async function prepareReportFinQualifications({root,context,verified,inputs,execute,literal}){
 if(!Array.isArray(inputs)||inputs.length>100)fail('REPORT_AB_MANAGED_INPUT_INVALID');
 if(!inputs.length)return []; // Existing releases receive no new evidence.
 if(context.environment!=='TEST'||context.expectedDatabase!=='cloudtms_test_clone')fail('REPORT_AB_MANAGED_SCOPE_INVALID');
 const catalogue=verified.evidence?.weekly_source_protected_qualification_catalogue;
 if(catalogue?.contract_sha256!==context.expectedHash||!hex.test(catalogue?.jsonb_sha256))fail('REPORT_AB_CATALOGUE_REQUIRED');
 const rows=[];
 for(const input of inputs){
  exact(input,['execution_release_id','decision','decision_binding'],'REPORT_AB_MANAGED_INPUT_INVALID');
  const recorded=JSON.parse(await execute(recordedReleaseSql(input.execution_release_id,literal)));
  if(!recorded||!Array.isArray(recorded.native_comparisons))fail('REPORT_AB_RECORDED_NATIVE_COMPARISON_REQUIRED');
  exact(recorded,['execution','native_comparisons'],'REPORT_AB_RECORDED_NATIVE_COMPARISON_REQUIRED');
  if(input.execution_release_id!==context.releaseId||recorded.execution.source_commit!==context.gitCommit
   ||recorded.execution.contract_sha256!==context.expectedHash||recorded.execution.managed_manifest_sha256!==context.manifestHash
   ||recorded.execution.installed_catalogue_jsonb_sha256!==catalogue.jsonb_sha256||!commit.test(context.receiptInputCommit))
   fail('REPORT_AB_INSTALLED_EXECUTION_CHANGED');
  const file=input.decision_binding.path;
  if(file!=='supabase/release/weekly-source-protected-operational.json'
   ||!/^\/report_fin_qualifications\/(0|[1-9][0-9]?)\/decision$/.test(input.decision_binding.json_pointer))fail('REPORT_AB_DECISION_BINDING_INVALID');
  const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,encoding:null,maxBuffer:256*1024*1024});
  const bytes=git(['show',context.receiptInputCommit+':'+file]),disk=fs.readFileSync(path.join(root,file));
  if(bytes.length>2*1024*1024||!bytes.equals(disk)||sha256(bytes)!==input.decision_binding.sha256)fail('REPORT_AB_REVIEWED_DECISION_CHANGED');
  const blob=git(['rev-parse',context.receiptInputCommit+':'+file]).toString().trim();
  const index=Number(input.decision_binding.json_pointer.split('/')[2]),bundle=JSON.parse(bytes.toString('utf8'));
  if(blob!==input.decision_binding.blob_oid||domainHash('REPORT_AB_COMMITTED_INPUT',bundle.report_fin_qualifications?.[index])
   !==domainHash('REPORT_AB_COMMITTED_INPUT',{execution_release_id:input.execution_release_id,decision:input.decision}))fail('REPORT_AB_REVIEWED_DECISION_CHANGED');
  const source=proveCommittedSources(root,recorded.execution.source_commit,context.receiptInputCommit);
  // Other scopes/history remain append-only. The actual committed decision
  // binds one complete exact scope batch; no body is silently selected/dropped.
  const requested=input.decision.native_comparison_sha256;
  const groups=new Map();for(const row of recorded.native_comparisons){const key=domainHash('REPORT_AB_SCOPE',row.scope);
   const group=groups.get(key)??[];group.push(row);groups.set(key,group);}
  const matches=[...groups.values()].map(group=>[...group].sort((a,b)=>a.case_id.localeCompare(b.case_id)||a.variant.localeCompare(b.variant)))
   .filter(group=>domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_COMPARISONS_V1',group)===requested);
  if(matches.length!==1)fail('REPORT_AB_RECORDED_NATIVE_COMPARISON_REQUIRED');
  rows.push(buildQualification(recorded.execution,recorded.execution,matches[0],input.decision,input.decision_binding,source,context.receiptInputCommit));
 }
 if(new Set(rows.map(q=>domainHash('REPORT_AB_SCOPE',q.scope))).size!==rows.length)fail('REPORT_AB_MANAGED_SCOPE_DUPLICATE');
 const old=JSON.parse(await execute("select coalesce(evidence_json->'weekly_source_report_fin_qualifications','null'::jsonb)::text "+
  'from private.cloudtms_database_releases where release_id='+literal(context.releaseId)));
 if(old!==null&&(!Array.isArray(old)||rows.some(q=>old.some(prior=>domainHash('REPORT_AB_SCOPE',prior.scope)===domainHash('REPORT_AB_SCOPE',q.scope)
  &&domainHash('REPORT_AB_QUALIFICATION_REPLAY',prior)!==domainHash('REPORT_AB_QUALIFICATION_REPLAY',q)))))fail('REPORT_AB_EVIDENCE_REPLACEMENT_REFUSED');
 return rows;
}
function committedReportJson(root,revision,file){
 if(!commit.test(revision)||(!Object.values(sourceInventory.receiptBundles).includes(file)&&file!==sourceInventory.manifest))
  fail('REPORT_AB_COMMITTED_PATH_INVALID');
 const bytes=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',revision+':'+file],
  {cwd:root,encoding:null,maxBuffer:32*1024*1024});
 if(!bytes.equals(fs.readFileSync(path.join(root,file))))fail('REPORT_AB_COMMITTED_BYTES_CHANGED');
 let decoded;try{decoded=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);}catch{fail('REPORT_AB_COMMITTED_UTF8_INVALID');}
 if(!Buffer.from(decoded).equals(bytes))fail('REPORT_AB_COMMITTED_UTF8_INVALID');
 return {bytes,value:JSON.parse(decoded)};
}
// Real default producer: re-read only the original bound receipt bundle/Git
// objects. No arbitrary prebuilt artifact array, file selector or code install.
export async function preparePostVerifiedReportRequest({root,expectedExecution,actorUserId,expectedEvidenceSha256,receiptInputCommit,
 artifactSlot,reason,turnBackendPid,execute,literal}){
 execution(expectedExecution);
 if(!uuid.test(actorUserId)||!hex.test(expectedEvidenceSha256)||!commit.test(receiptInputCommit)
  ||!['weekly_source_report_fin_native_comparisons','weekly_source_report_fin_qualifications'].includes(artifactSlot)
  ||typeof reason!=='string'||reason.trim()!==reason||reason.length<1||reason.length>1000
  ||!Number.isSafeInteger(turnBackendPid)||turnBackendPid<1||typeof execute!=='function'||typeof literal!=='function')
  fail('REPORT_AB_POST_VERIFIED_INPUT_INVALID');
 const proof=proveCommittedSources(root,expectedExecution.source_commit,receiptInputCommit);
 const {value:manifest}=committedReportJson(root,receiptInputCommit,sourceInventory.manifest);
 if(manifest.releaseId!==expectedExecution.release_id)fail('REPORT_AB_COMMITTED_RELEASE_RELABELLED');
 const descriptor=manifest.weeklySourceProtectedOperational,file=sourceInventory.receiptBundles.weeklySourceProtectedOperational;
 exact(descriptor,['path','sha256'],'REPORT_AB_COMMITTED_DESCRIPTOR_INVALID');
 if(descriptor.path!==file||!hex.test(descriptor.sha256))fail('REPORT_AB_COMMITTED_DESCRIPTOR_INVALID');
 const {bytes,value:bundle}=committedReportJson(root,receiptInputCommit,file);
 exact(bundle,['schema_version','qualifications',...(bundle.report_fin_qualifications===undefined?[]:['report_fin_qualifications']),
  ...(bundle.report_fin_native_inputs===undefined?[]:['report_fin_native_inputs'])],'REPORT_AB_COMMITTED_BUNDLE_INVALID');
 if(sha256(bytes)!==descriptor.sha256||bundle.schema_version!=='WEEKLY_SOURCE_PROTECTED_OPERATIONAL_INPUT_BUNDLE_V1'
  ||!Array.isArray(bundle.qualifications)||bundle.qualifications.length>100)fail('REPORT_AB_COMMITTED_BUNDLE_INVALID');
 const context={environment:'TEST',expectedDatabase:expectedExecution.database,mode:'UPGRADE',releaseId:expectedExecution.release_id,
  gitCommit:expectedExecution.source_commit,receiptInputCommit,expectedHash:expectedExecution.contract_sha256,
  manifestHash:expectedExecution.managed_manifest_sha256};
 let artifacts;
 if(artifactSlot==='weekly_source_report_fin_native_comparisons'){
  if(!Array.isArray(bundle.report_fin_native_inputs)||!bundle.report_fin_native_inputs.length||bundle.report_fin_native_inputs.length>14)
   fail('REPORT_AB_COMMITTED_NATIVE_INPUT_REQUIRED');
  artifacts=buildNativeComparisons(expectedExecution,bundle.report_fin_native_inputs.map(input=>{
   exact(input,['receipt_id','receipt','receipt_sha256'],'REPORT_AB_COMMITTED_NATIVE_INPUT_INVALID');
   if(typeof input.receipt!=='string')fail('REPORT_AB_COMMITTED_NATIVE_INPUT_INVALID');
   return {receipt_id:input.receipt_id,receipt_bytes:Buffer.from(input.receipt,'utf8'),receipt_sha256:input.receipt_sha256};
  }));
 }else{
  const inputs=loadReportFinInputs(root,manifest,context);
  if(!inputs.length)fail('REPORT_AB_COMMITTED_DECISION_REQUIRED');
  // These two fields come from the actual native execution reader's current
  // catalogue binding, not an invented successful verifier/financial receipt.
  const verified={evidence:{weekly_source_protected_qualification_catalogue:{contract_sha256:expectedExecution.contract_sha256,
   jsonb_sha256:expectedExecution.installed_catalogue_jsonb_sha256}}};
  artifacts=await prepareReportFinQualifications({root,context,verified,inputs,execute,literal});
 }
 return {schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECORD_V1',expected_execution:expectedExecution,
  expected_evidence_sha256:expectedEvidenceSha256,receipt_input_commit:receiptInputCommit,receipt_tree_proof:proof,
  artifact_slot:artifactSlot,artifacts,reason,turn_backend_pid:turnBackendPid};
}
// Root-only source interface to Money's actual owner-session recorder. It
// neither installs/resumes a release nor writes the release ledger directly.
// Exact recorder/receipt ABI remains provisional until Money's full freeze.
export const postVerifiedReceiptKeys=Object.freeze(['schema_version','execution','receipt_input_commit','receipt_tree_sha256',
 'artifact_slot','artifacts_sha256','prior_evidence_sha256','evidence_sha256','actor_user_id','recorded_at_utc',
 'replayed','operational_settings_changed','receipt_sha256']);
export function assertPostVerifiedReportReceipt(result,request,actorUserId){
 try{
 exact(result,postVerifiedReceiptKeys,'REPORT_AB_POST_VERIFIED_RECEIPT_INVALID');
 const {receipt_sha256,...body}=result;
 const stamp=result.recorded_at_utc;
 if(result.schema_version!=='WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1'
  ||domainHash('REPORT_AB_EXECUTION',result.execution)!==domainHash('REPORT_AB_EXECUTION',request.expected_execution)
  ||result.receipt_input_commit!==request.receipt_input_commit
  ||result.receipt_tree_sha256!==request.receipt_tree_proof.source_equivalence_sha256
  ||result.artifact_slot!==request.artifact_slot
  ||result.artifacts_sha256!==domainHash('WEEKLY_SOURCE_POST_VERIFIED_ARTIFACT_SET_V1',request.artifacts)
  ||result.prior_evidence_sha256!==request.expected_evidence_sha256||!hex.test(result.evidence_sha256)
  ||result.actor_user_id!==actorUserId||typeof result.replayed!=='boolean'||result.operational_settings_changed!==false
  ||(result.replayed!==(result.prior_evidence_sha256===result.evidence_sha256))
  ||typeof stamp!=='string'||!/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(stamp)
  ||!date(stamp.slice(0,10))||!Number.isFinite(Date.parse(stamp))
  ||receipt_sha256!==domainHash('WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1',body))
  fail('REPORT_AB_POST_VERIFIED_RECEIPT_INVALID');
 // New evidence is not reconstructed from an invented empty prior ledger.
 // Root's controlling recorder/readback owns the full evidence body and CAS.
 return result;
 }catch{fail('REPORT_AB_POST_VERIFIED_RECEIPT_INVALID');}
}
export async function recordPostVerifiedReportArtifacts({actorUserId,expectedExecution,expectedEvidenceSha256,receiptInputCommit,
 receiptTreeProof,artifactSlot,artifacts,reason,turnBackendPid,execute,literal}){
 execution(expectedExecution);
 if(!uuid.test(actorUserId)||!hex.test(expectedEvidenceSha256)||!commit.test(receiptInputCommit)
  ||!['weekly_source_report_fin_native_comparisons','weekly_source_report_fin_qualifications'].includes(artifactSlot)
  ||!Array.isArray(artifacts)||!artifacts.length||artifacts.length>100||typeof reason!=='string'||reason.trim()!==reason||reason.length<1||reason.length>1000
  ||!Number.isSafeInteger(turnBackendPid)||turnBackendPid<1||typeof execute!=='function'||typeof literal!=='function')fail('REPORT_AB_POST_VERIFIED_INPUT_INVALID');
 assertExecutionTreeProof(receiptTreeProof,expectedExecution.source_commit,receiptInputCommit);
 if(artifactSlot==='weekly_source_report_fin_native_comparisons')assertNativeComparisons(expectedExecution,artifacts);
 else {
  for(const q of artifacts){
   exact(q,['schema_version','phase','installation','receipt_input_commit','scope','execution','native_comparisons','decision',
    'decision_binding','source_equivalence','qualification_sha256'],'REPORT_AB_POST_VERIFIED_CERTIFICATE_INVALID');
   if(q.receipt_input_commit!==receiptInputCommit
    ||domainHash('REPORT_AB_EXECUTION',q.installation)!==domainHash('REPORT_AB_EXECUTION',expectedExecution)
    ||domainHash('REPORT_AB_EXECUTION',q.execution)!==domainHash('REPORT_AB_EXECUTION',expectedExecution))
    fail('REPORT_AB_POST_VERIFIED_CERTIFICATE_INVALID');
   const rebuilt=buildQualification(q.installation,q.execution,q.native_comparisons,q.decision,q.decision_binding,
    q.source_equivalence,q.receipt_input_commit);
   if(domainHash('REPORT_AB_CERTIFICATE_REBUILD',rebuilt)!==domainHash('REPORT_AB_CERTIFICATE_REBUILD',q))
    fail('REPORT_AB_POST_VERIFIED_CERTIFICATE_INVALID');
  }
  if(new Set(artifacts.map(q=>domainHash('REPORT_AB_SCOPE',q.scope))).size!==artifacts.length)
   fail('REPORT_AB_POST_VERIFIED_CERTIFICATE_INVALID');
 }
 const request={schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECORD_V1',expected_execution:expectedExecution,
  expected_evidence_sha256:expectedEvidenceSha256,receipt_input_commit:receiptInputCommit,receipt_tree_proof:receiptTreeProof,
  artifact_slot:artifactSlot,artifacts,reason,turn_backend_pid:turnBackendPid};
 const returned=await execute('select private.weekly_source_post_verified_evidence_record_v1('+literal(actorUserId)+'::uuid,'+
  sqlJson(request,literal)+')::text;');
 let result;try{result=JSON.parse(returned);}catch{fail('REPORT_AB_POST_VERIFIED_RECEIPT_INVALID');}
 return assertPostVerifiedReportReceipt(result,request,actorUserId);
}
const ownerLiteral=value=>"'"+String(value).replaceAll("'","''")+"'";
// Root supplies one fresh owned session and the separate actual SEND7 lock PID.
// This module creates no connection and performs no effect when imported.
export async function recordPostVerifiedReportEvidence({session,root,actorUserId,receiptInputCommit,artifactSlot,reason,turnBackendPid}){
 if(!session||typeof session.execute!=='function')fail('REPORT_AB_OWNER_SESSION_REQUIRED');
 let held=false,begun=false,committed=false;
 try{
  const acquired=await session.execute("select pg_try_advisory_lock(hashtextextended('cloudtms_database_release_admission_v1',0));");
  if(acquired.trim()!=='t')fail('REPORT_AB_MANAGED_WRITER_BUSY');held=true;
  begun=true;await session.execute("begin; set local lock_timeout='5s'; set local statement_timeout='120s'; select private.weekly_source_protected_qualification_owner_v1();");
  const state=JSON.parse(await session.execute("select jsonb_build_object('execution',private.weekly_source_protected_qualification_release_v1(),"
   +"'evidence_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_RELEASE_EVIDENCE_V1',r.evidence_json),'hex'))::text "
   +"from private.cloudtms_database_releases r order by started_at_utc desc,release_id desc limit 1;"));
  exact(state,['execution','evidence_sha256'],'REPORT_AB_OWNER_STATE_INVALID');
  const request=await preparePostVerifiedReportRequest({root,expectedExecution:state.execution,actorUserId,
   expectedEvidenceSha256:state.evidence_sha256,receiptInputCommit,artifactSlot,reason,turnBackendPid,
   execute:sql=>session.execute(sql),literal:ownerLiteral});
  const receipt=await recordPostVerifiedReportArtifacts({actorUserId,expectedExecution:state.execution,
   expectedEvidenceSha256:state.evidence_sha256,receiptInputCommit,receiptTreeProof:request.receipt_tree_proof,
   artifactSlot,artifacts:request.artifacts,reason,turnBackendPid,execute:sql=>session.execute(sql),literal:ownerLiteral});
  await session.execute('commit;');committed=true;return receipt;
 }finally{
  if(begun&&!committed)try{await session.execute('rollback;');}catch{}
  // An uncertain commit/connection remains root-owned; never open an unguarded
  // replacement writer or claim fresh rollback/financial acceptance here.
  if(held)try{await session.execute("select pg_advisory_unlock(hashtextextended('cloudtms_database_release_admission_v1',0));");}catch{}
 }
}
