import crypto from 'node:crypto';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,hex=/^[0-9a-f]{64}$/;
const fail=code=>{throw Error(code);};
export const sha256=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
export const exact=(value,keys,code)=>{if(!value||Array.isArray(value)||typeof value!=='object'||Object.keys(value).sort().join('|')!==[...keys].sort().join('|'))fail(code);};
// This small qualification DTO contains strings, booleans, null and arrays
// only. JSONB orders object keys by UTF8 byte length then bytes. No numeric
// economic serialization is implemented here; the database owns Source CAS.
export function pgJsonbText(value){
 if(value===null||typeof value==='boolean'||typeof value==='string')return JSON.stringify(value);
 if(Array.isArray(value))return '['+value.map(pgJsonbText).join(', ')+']';
 if(value&&typeof value==='object')return '{'+Object.keys(value).sort((a,b)=>Buffer.byteLength(a)-Buffer.byteLength(b)||Buffer.compare(Buffer.from(a),Buffer.from(b)))
  .map(key=>JSON.stringify(key)+': '+pgJsonbText(value[key])).join(', ')+'}';
 fail('QUALIFICATION_JSON_NUMERIC_OR_UNDEFINED');
}
export const domainHash=(domain,value)=>sha256(domain+'\x1f'+pgJsonbText(value));
export const gates=Object.freeze({
 SOURCE_CAS_NATIVE:['sorted_original_clients','remaining_client_after_first_complete','source_change_40001','own_stage_cas_unchanged','source_receipt_identity_checked'],
 NATIVE_PROTECTION:['exact_signed_claim','native_prepare_stage_complete','pending_recovery','partial_retry_truth','same_family_rounds','cross_candidate_rounds'],
 SECURITY:['active_admin_activation','service_only_capability','browser_denied','actor_revoked','foreign_group_refused'],
 LEGACY_PAY:['native_authority_retained','source_pay_unchanged','no_payment_provider_calls','no_external_effects'],
 NEXT_PAY:['native_i1_complete_set','candidate_gate_first','growth_40001','native_next_hooks_retained','empty_lock_set_guard'],
 CLIENT_API:['trusted_actor_injected','closed_browser_payload','bulk_preview_commit','partial_retry_receipt','source_cas_error_order'],
 DOCUMENT:['native_snapshot_qualification','final_issue_frozen_hash','source_receipt_membership','legacy_renderer_qualified','no_network_delivery'],
 OWN_STAGE_CAS:['exact_pending_request','only_own_stage_excluded','stage_tamper_refused','completed_client_retained','foreign_stage_refused'],
 SOURCE_ROUTING:['null_rules_unchanged','non_self_bill_bulk_scope','nhsp_excluded','signed_scope_retained','window_original_pay_only'],
 AUTOMATIC_COMPONENT_DELTA:['literal_component_census','native_before_after','money_delta_zero','structural_delta_zero','mismatch_manual_only','pending_replay_no_recalculate']
});
export function validateQualificationContext(context){
 exact(context,['release_id','git_commit','contract_sha256','agency_id','environment','database','source_group_id','effective_from_week_ending','effective_to_week_ending','capabilities'],'QUALIFICATION_CONTEXT_INVALID');
 if(context.environment!=='TEST'||context.database!=='cloudtms_test_clone'||!/^[0-9a-f]{40}$/.test(context.git_commit)
  ||!hex.test(context.contract_sha256)||!uuid.test(context.agency_id)||!uuid.test(context.source_group_id)
  ||typeof context.release_id!=='string'||!context.release_id||!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(context.effective_from_week_ending)
  ||(context.effective_to_week_ending!==null&&!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(context.effective_to_week_ending))
  ||!Array.isArray(context.capabilities)||!context.capabilities.length||context.capabilities.length>2
  ||new Set(context.capabilities).size!==context.capabilities.length||context.capabilities.some(x=>!['BULK_SIGNED','AUTOMATIC_RECONCILE'].includes(x))
  ||(context.capabilities.includes('AUTOMATIC_RECONCILE')&&!context.capabilities.includes('BULK_SIGNED')))fail('QUALIFICATION_CONTEXT_INVALID');
}
// Root's actual hosted journey collector supplies byte-preserved receipts.
// A definitions/ACL compile or a locally simulated PASS cannot qualify. The
// saved native tool envelope and exact assertion receipts are both required.
export function buildQualification(context,inputs){
 validateQualificationContext(context);
 const needed=Object.keys(gates).filter(gate=>gate!=='AUTOMATIC_COMPONENT_DELTA'||context.capabilities.includes('AUTOMATIC_RECONCILE'));
 if(!Array.isArray(inputs)||inputs.length!==needed.length||new Set(inputs.map(x=>x.gate)).size!==needed.length)fail('QUALIFICATION_GATE_CENSUS_INVALID');
 const receipts=inputs.map(input=>{
  exact(input,['gate','receipt_id','receipt_bytes','expected_receipt_sha256'],'QUALIFICATION_INPUT_INVALID');
  if(!needed.includes(input.gate)||typeof input.receipt_id!=='string'||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(input.receipt_id)
    ||!(Buffer.isBuffer(input.receipt_bytes)||typeof input.receipt_bytes==='string')||sha256(input.receipt_bytes)!==input.expected_receipt_sha256)fail('QUALIFICATION_ARTIFACT_INVALID');
  const receipt=JSON.parse(input.receipt_bytes.toString('utf8'));
  exact(receipt,['schema_version','gate','release_id','source_commit','contract_sha256','agency_id','environment','database','source_group_id',
    'effective_from_week_ending','effective_to_week_ending','execution','native_execution_receipt','assertions'],'QUALIFICATION_NATIVE_RECEIPT_INVALID');
  if(receipt.schema_version!=='WEEKLY_SOURCE_NATIVE_JOURNEY_RECEIPT_V1'||receipt.gate!==input.gate||receipt.execution!=='AGENCY_TEST_NATIVE'
   ||receipt.release_id!==context.release_id||receipt.source_commit!==context.git_commit||receipt.contract_sha256!==context.contract_sha256
   ||['agency_id','environment','database','source_group_id','effective_from_week_ending','effective_to_week_ending'].some(key=>receipt[key]!==context[key]))fail('QUALIFICATION_NATIVE_SCOPE_INVALID');
  const execution=receipt.native_execution_receipt;
  exact(execution,['accepted','database','rolledBack','connectedFresh','tool_receipt_sha256','native_journey_id'],'QUALIFICATION_NATIVE_EXECUTION_INVALID');
  if(execution.accepted!==true||execution.database!==context.database||execution.rolledBack!==true||execution.connectedFresh!==true
   ||!hex.test(execution.tool_receipt_sha256)||execution.native_journey_id!=='PROTECTED_CAPABILITY_'+input.gate)fail('QUALIFICATION_NATIVE_EXECUTION_INVALID');
  exact(receipt.assertions,gates[input.gate],'QUALIFICATION_ASSERTION_CENSUS_INVALID');
  if(Object.values(receipt.assertions).some(value=>value!==true))fail('QUALIFICATION_NATIVE_ASSERTION_FAILED');
  return {gate:input.gate,status:'PASS',execution:'AGENCY_TEST_NATIVE',database:context.database,receipt_id:input.receipt_id,
   receipt_sha256:input.expected_receipt_sha256,source_commit:context.git_commit,contract_sha256:context.contract_sha256,
   source_group_id:context.source_group_id,agency_id:context.agency_id,environment:context.environment};
 }).sort((a,b)=>a.gate.localeCompare(b.gate));
 const q={schema_version:'WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1',release_id:context.release_id,git_commit:context.git_commit,
  contract_sha256:context.contract_sha256,agency_id:context.agency_id,environment:context.environment,source_group_id:context.source_group_id,
  effective_from_week_ending:context.effective_from_week_ending,effective_to_week_ending:context.effective_to_week_ending,
  capabilities:[...context.capabilities].sort(),gate_receipts:receipts};
 return {...q,qualification_sha256:domainHash('WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1',q)};
}
export function assertManagedQualificationEvidence(context,evidence){
 const rows=evidence?.weekly_source_protected_capability_qualifications;
 if(rows===undefined)return; // All existing releases stay byte-identical.
 if(context.environment!=='TEST'||context.expectedDatabase!=='cloudtms_test_clone'||!Array.isArray(rows)||rows.length>100)fail('QUALIFICATION_MANAGED_SCOPE_INVALID');
 const seen=new Set();
 for(const q of rows){
  exact(q,['schema_version','release_id','git_commit','contract_sha256','agency_id','environment','source_group_id','effective_from_week_ending',
   'effective_to_week_ending','capabilities','gate_receipts','qualification_sha256'],'QUALIFICATION_MANAGED_RECEIPT_INVALID');
  validateQualificationContext({release_id:q.release_id,git_commit:q.git_commit,contract_sha256:q.contract_sha256,agency_id:q.agency_id,
   environment:q.environment,database:context.expectedDatabase,source_group_id:q.source_group_id,effective_from_week_ending:q.effective_from_week_ending,
   effective_to_week_ending:q.effective_to_week_ending,capabilities:q.capabilities});
  if(q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1'||q.release_id!==context.releaseId||q.git_commit!==context.gitCommit
    ||q.contract_sha256!==context.expectedHash||!hex.test(q.qualification_sha256))fail('QUALIFICATION_MANAGED_RECEIPT_INVALID');
  const {qualification_sha256,...body}=q;
  if(domainHash('WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1',body)!==qualification_sha256)fail('QUALIFICATION_MANAGED_HASH_INVALID');
  const needed=Object.keys(gates).filter(gate=>gate!=='AUTOMATIC_COMPONENT_DELTA'||q.capabilities.includes('AUTOMATIC_RECONCILE'));
  if(!Array.isArray(q.gate_receipts)||q.gate_receipts.length!==needed.length||new Set(q.gate_receipts.map(x=>x.gate)).size!==needed.length)fail('QUALIFICATION_GATE_CENSUS_INVALID');
  for(const receipt of q.gate_receipts){
   exact(receipt,['gate','status','execution','database','receipt_id','receipt_sha256','source_commit','contract_sha256','source_group_id','agency_id','environment'],'QUALIFICATION_MANAGED_RECEIPT_INVALID');
   if(!needed.includes(receipt.gate)||receipt.status!=='PASS'||receipt.execution!=='AGENCY_TEST_NATIVE'||receipt.database!==context.expectedDatabase
    ||receipt.source_commit!==q.git_commit||receipt.contract_sha256!==q.contract_sha256||receipt.source_group_id!==q.source_group_id
    ||receipt.agency_id!==q.agency_id||receipt.environment!==q.environment||typeof receipt.receipt_id!=='string'
    ||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(receipt.receipt_id)||!hex.test(receipt.receipt_sha256))fail('QUALIFICATION_MANAGED_RECEIPT_INVALID');
  }
  const key=q.source_group_id+':'+q.effective_from_week_ending+':'+q.effective_to_week_ending;
  if(seen.has(key))fail('QUALIFICATION_MANAGED_SCOPE_DUPLICATE');seen.add(key);
 }
}
