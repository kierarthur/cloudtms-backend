import {domainHash,exact} from './weekly-source-protected-release-qualification.mjs';
import {sourceOperations,automaticOperations} from '../broker/src/weekly-source/off-rule-native-proxy.mjs';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,hex=/^[0-9a-f]{64}$/;
const fail=code=>{throw Error(code);};
export const offRuleSchema='WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1';
const inputKeys=['authorization_id','scope_id','agency_id','source_group_id','source_cycle_id','setup_actor_user_id','journey_actor_user_id',
 'purpose','week_ending','expires_at_utc','fixture_namespace','fixture_manifest','assignment_id','assignment_version','assignment_sha256',
 'rule_set_version_id','rule_values_sha256','original_source_cas','allowed_operations','banking_install_receipt_sha256','security_receipt_sha256','source_effects_review_sha256'];
function sortedIds(rows){return Array.isArray(rows)&&rows.length>0&&rows.length<=100&&rows.every(id=>uuid.test(id))
 &&JSON.stringify(rows)===JSON.stringify([...new Set(rows)].sort());}
function validDate(value){return typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)
 &&Number.isFinite(Date.parse(value+'T00:00:00Z'))&&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value;}
function validateInput(input,controls){
 exact(input,inputKeys,'OFF_RULE_SETUP_INPUT_INVALID');
 if(!['authorization_id','scope_id','agency_id','source_group_id','source_cycle_id','setup_actor_user_id','journey_actor_user_id','assignment_id','rule_set_version_id'].every(k=>uuid.test(input[k]))
  ||!['SOURCE_FIXTURE_SETUP','AUTOMATIC_JOURNEY'].includes(input.purpose)||input.fixture_namespace!=='WSQ_TEST_'
  ||!validDate(input.week_ending)||new Date(input.week_ending+'T00:00:00Z').getUTCDay()!==0
  ||typeof input.expires_at_utc!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(input.expires_at_utc)
  ||!Number.isFinite(Date.parse(input.expires_at_utc))||!['assignment_sha256','rule_values_sha256','banking_install_receipt_sha256','security_receipt_sha256','source_effects_review_sha256'].every(k=>hex.test(input[k]))
  ||typeof input.assignment_version!=='string'||!/^[1-9][0-9]*$/.test(input.assignment_version))fail('OFF_RULE_SETUP_INPUT_INVALID');
 exact(controls,['schema_version','release_id','source_commit','contract_sha256','banking_install_receipt_sha256','security_receipt_sha256','source_effects_review_sha256',
  'banking_first_install_complete','effective_acl_pass','browser_denied','source_effects_no_external_calls'],'OFF_RULE_SETUP_CONTROLS_INVALID');
 if(controls.schema_version!=='WEEKLY_SOURCE_OFF_RULE_TEST_CONTROLS_V1'
  ||!['banking_first_install_complete','effective_acl_pass','browser_denied','source_effects_no_external_calls'].every(k=>controls[k]===true)
  ||['banking_install_receipt_sha256','security_receipt_sha256','source_effects_review_sha256'].some(k=>controls[k]!==input[k]))fail('OFF_RULE_SETUP_CONTROLS_REQUIRED');
 const manifest=input.fixture_manifest;
 exact(manifest,['group_code','client_ids','contract_ids','candidate_ids','client_refs','candidate_refs'],'OFF_RULE_SETUP_FIXTURE_INVALID');
 if(!/^WSQ_TEST_[A-Z0-9_]+$/.test(manifest.group_code)||!['client_ids','contract_ids','candidate_ids'].every(k=>sortedIds(manifest[k])))fail('OFF_RULE_SETUP_FIXTURE_INVALID');
 for(const [field,idKey,refKey,idList,pattern]of [['client_refs','client_id','cli_ref','client_ids',/^CLI-913013[0-9]{2}$/],
  ['candidate_refs','candidate_id','tms_ref','candidate_ids',/^CCR-913013[0-9]{2}$/]]){
  const rows=manifest[field];if(!Array.isArray(rows)||rows.length!==manifest[idList].length)fail('OFF_RULE_SETUP_FIXTURE_INVALID');
  for(const row of rows){exact(row,[idKey,refKey],'OFF_RULE_SETUP_FIXTURE_INVALID');if(!uuid.test(row[idKey])||!pattern.test(row[refKey]))fail('OFF_RULE_SETUP_FIXTURE_INVALID');}
  if(JSON.stringify(rows.map(row=>row[idKey]))!==JSON.stringify(manifest[idList])||new Set(rows.map(row=>row[refKey])).size!==rows.length)fail('OFF_RULE_SETUP_FIXTURE_INVALID');
 }
 const allowed=input.purpose==='SOURCE_FIXTURE_SETUP'?sourceOperations:automaticOperations;
 if(!Array.isArray(input.allowed_operations)||!input.allowed_operations.length||input.allowed_operations.some(name=>!allowed.includes(name))
  ||JSON.stringify(input.allowed_operations)!==JSON.stringify([...new Set(input.allowed_operations)].sort()))fail('OFF_RULE_SETUP_OPERATIONS_INVALID');
 if(input.purpose==='SOURCE_FIXTURE_SETUP'){
  if(input.original_source_cas!==null)fail('OFF_RULE_SETUP_SOURCE_BEFORE_FINAL_REQUIRED');
 }else{
  const cas=input.original_source_cas;exact(cas,['schema_version','source_cycle_id','client_ids','source_cas_sha256'],'OFF_RULE_SETUP_SOURCE_CAS_INVALID');
  if(manifest.client_ids.length!==1||cas.schema_version!=='WEEKLY_SOURCE_BULK_SOURCE_CAS_V1'||cas.source_cycle_id!==input.source_cycle_id
   ||JSON.stringify(cas.client_ids)!==JSON.stringify(manifest.client_ids)||!hex.test(cas.source_cas_sha256))fail('OFF_RULE_SETUP_SOURCE_CAS_INVALID');
 }
}
// Existing protected managed evidence loader calls this with real captured
// controls. It must not accept browser inputs or infer controls from presence.
// SQL independently validates current release/installed controls/audit/config.
export function buildOffRuleTestAuthorization(context,input,controls){
 validateInput(input,controls);
 if(context.environment!=='TEST'||context.expectedDatabase!=='cloudtms_test_clone'||context.mode!=='UPGRADE'
  ||controls.release_id!==context.releaseId||controls.source_commit!==context.gitCommit||controls.contract_sha256!==context.expectedHash
  ||!/^[0-9a-f]{40}$/.test(context.gitCommit)||!hex.test(context.expectedHash))fail('OFF_RULE_SETUP_EXECUTION_INVALID');
 const body={schema_version:offRuleSchema,...input,release_id:context.releaseId,source_commit:context.gitCommit,contract_sha256:context.expectedHash};
 return {...body,authorization_sha256:domainHash(offRuleSchema,body)};
}
export function assertManagedOffRuleTestEvidence(context,evidence){
 const controls=evidence.weekly_source_off_rule_test_controls;
 const rows=(evidence.weekly_source_protected_test_setup_authorizations??[]).filter(row=>row?.schema_version===offRuleSchema);
 if(rows.length===0)return;
 if(rows.length>20||new Set(rows.map(row=>[row.source_cycle_id,row.journey_actor_user_id,row.purpose].join(':'))).size!==rows.length)fail('OFF_RULE_SETUP_DUPLICATE_SCOPE');
 for(const row of rows){
  exact(row,['schema_version',...inputKeys,'release_id','source_commit','contract_sha256','authorization_sha256'],'OFF_RULE_SETUP_MANAGED_INPUT_INVALID');
  const input=Object.fromEntries(inputKeys.map(key=>[key,row[key]]));
  const measured=buildOffRuleTestAuthorization(context,input,controls);
  if(domainHash('OFF_RULE_EXACT_MANAGED_DTO',row)!==domainHash('OFF_RULE_EXACT_MANAGED_DTO',measured))fail('OFF_RULE_SETUP_MANAGED_INPUT_INVALID');
 }
}
