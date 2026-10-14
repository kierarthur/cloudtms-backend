import {domainHash,exact} from '../protected-source-capability/release-qualification.mjs';
import {loadCommittedBundle} from './source-equivalence.mjs';
const fail=code=>{throw Error(code);};
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,hex=/^[0-9a-f]{64}$/;
const keys=['authorization_id','scope_id','agency_id','source_group_id','source_cycle_id','setup_actor_user_id','journey_actor_user_id',
 'week_ending','expires_at_utc','fixture_namespace','fixture_manifest','original_census','prerequisite_qualification_sha256','capabilities'];
export function buildTestSetupAuthorization(context,input,qualifications){
 exact(input,keys,'TEST_SETUP_INPUT_INVALID');
 if(context.environment!=='TEST'||context.expectedDatabase!=='cloudtms_test_clone'||context.mode!=='UPGRADE'
  ||['authorization_id','scope_id','agency_id','source_group_id','source_cycle_id','setup_actor_user_id','journey_actor_user_id'].some(k=>!uuid.test(input[k]))
  ||input.fixture_namespace!=='WSQ_TEST_'||JSON.stringify(input.capabilities)!=='["BULK_SIGNED"]'
  ||!/^\d{4}-\d{2}-\d{2}$/.test(input.week_ending)||!Number.isFinite(Date.parse(input.expires_at_utc))
  ||!hex.test(input.prerequisite_qualification_sha256))fail('TEST_SETUP_INPUT_INVALID');
 const manifest=input.fixture_manifest;
 exact(manifest,['group_code','client_ids','candidate_ids','contract_ids','original_root_ids','work_event_ids','client_refs','candidate_refs'],'TEST_SETUP_FIXTURE_MANIFEST_INVALID');
 if(typeof manifest.group_code!=='string'||!/^WSQ_TEST_[A-Z0-9_]+$/.test(manifest.group_code))fail('TEST_SETUP_FIXTURE_MANIFEST_INVALID');
 for(const field of ['client_ids','candidate_ids','contract_ids','original_root_ids','work_event_ids']){
  const rows=manifest[field];
  if(!Array.isArray(rows)||!rows.length||rows.length>100||rows.some(id=>!uuid.test(id))
   ||JSON.stringify(rows)!==JSON.stringify([...new Set(rows)].sort()))fail('TEST_SETUP_FIXTURE_MANIFEST_INVALID');
 }
 for(const [field,idKey,refKey,pattern,idList]of [['client_refs','client_id','cli_ref',/^CLI-913013[0-9]{2}$/,'client_ids'],
  ['candidate_refs','candidate_id','tms_ref',/^CCR-913013[0-9]{2}$/,'candidate_ids']]){
  const refs=manifest[field];if(!Array.isArray(refs)||refs.length!==manifest[idList].length)fail('TEST_SETUP_FIXTURE_REFERENCE_INVALID');
  for(const row of refs){exact(row,[idKey,refKey],'TEST_SETUP_FIXTURE_REFERENCE_INVALID');
   if(!uuid.test(row[idKey])||!pattern.test(row[refKey]))fail('TEST_SETUP_FIXTURE_REFERENCE_INVALID');}
  if(JSON.stringify(refs.map(row=>row[idKey]))!==JSON.stringify(manifest[idList])
   ||new Set(refs.map(row=>row[refKey])).size!==refs.length)fail('TEST_SETUP_FIXTURE_REFERENCE_INVALID');
 }
 const census=input.original_census;
 exact(census,['schema_version','source_cycle_id','source_group_id','week_ending','original_client_ids','original_root_identities','original_items',
  'source_cas','census_sha256'],'TEST_SETUP_CENSUS_INVALID');
 const {census_sha256,...censusBody}=census;
 if(census.schema_version!=='WEEKLY_SOURCE_QUALIFICATION_CENSUS_V1'||census.source_cycle_id!==input.source_cycle_id
  ||census.source_group_id!==input.source_group_id||census.week_ending!==input.week_ending
  ||domainHash(census.schema_version,censusBody)!==census_sha256
  ||JSON.stringify(census.original_client_ids)!==JSON.stringify(manifest.client_ids))fail('TEST_SETUP_CENSUS_INVALID');
 const receipts=qualifications.filter(q=>q.qualification_sha256===input.prerequisite_qualification_sha256);
 if(receipts.length!==1)fail('TEST_SETUP_PREREQUISITES_REQUIRED');
 const q=receipts[0];
 if(q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2'
  ||q.phase!=='PREREQUISITES_QUALIFIED_OPERATIONAL_REPLAY_PENDING'||q.source_group_id!==input.source_group_id
  ||q.agency_id!==input.agency_id||q.release_id!==context.releaseId||q.git_commit!==context.gitCommit
  ||q.contract_sha256!==context.expectedHash||JSON.stringify(q.capabilities)!=='["BULK_SIGNED"]'
  ||input.week_ending<q.effective_from_week_ending||(q.effective_to_week_ending!==null&&input.week_ending>q.effective_to_week_ending)
  ||q.gate_receipts.length!==9||!q.gate_receipts.some(row=>row.gate==='NEXT_PAY'&&row.status==='PREREQUISITE_PASS'))
  fail('TEST_SETUP_PREREQUISITES_REQUIRED');
 const body={schema_version:'WEEKLY_SOURCE_PROTECTED_TEST_SETUP_AUTHORIZATION_V1',...input,
  release_id:context.releaseId,source_commit:context.gitCommit,contract_sha256:context.expectedHash};
 return {...body,authorization_sha256:domainHash(body.schema_version,body)};
}
export function loadTestSetupInputs(root,release,context){
 const bundle=loadCommittedBundle(root,release,context,'weeklySourceProtectedTestSetup');if(bundle===null)return [];
 exact(bundle,['schema_version','authorizations'],'TEST_SETUP_BUNDLE_INVALID');
 if(bundle.schema_version!=='WEEKLY_SOURCE_PROTECTED_TEST_SETUP_INPUT_BUNDLE_V1'||!Array.isArray(bundle.authorizations)
  ||!bundle.authorizations.length||bundle.authorizations.length>10)fail('TEST_SETUP_BUNDLE_INVALID');
 if(new Set(bundle.authorizations.map(a=>a.scope_id)).size!==bundle.authorizations.length)fail('TEST_SETUP_DUPLICATE_SCOPE');
 return bundle.authorizations;
}
