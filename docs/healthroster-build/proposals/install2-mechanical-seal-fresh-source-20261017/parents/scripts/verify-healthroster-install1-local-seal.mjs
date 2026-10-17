// Local mechanical contract/first-use verification; no hosted target is accepted.
import fs from 'node:fs';import path from 'node:path';
import {psql,repoRoot,releaseVerifierVariables,canonicalContractHash,readJson} from './cloudtms-db-release-lib.mjs';
import {digest} from './cloudtms-managed-release-sql.mjs';
const target=new URL(process.env.CLOUDTMS_DATABASE_URL??'');
if(target.hostname!=='127.0.0.1'||target.port!=='55447'||target.pathname!=='/banking_modal_v2_test'
 ||target.username!=='cloudtms_hr_install1_local_owner'||process.env.CLOUDTMS_ALLOW_LOCAL!=='1'
 ||process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER!=='CURRENT_USER')throw Error('OWN_LOCAL_SEAL_VERIFICATION_TARGET_REFUSED');
const files=['supabase/verification/13102026_1126_weekly_source_protected_pay_authority_verification.sql',
 'supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql',
 'supabase/verification/22082026_1302_general_browser_isolation_verification.sql',
 'supabase/verification/27082026_1947_candidate_named_security_verification_v3.sql'];
const beganAt=new Date().toISOString(),results=[];
try{
 for(const file of files){
  const startedAt=new Date().toISOString();
  try{psql({file,variables:releaseVerifierVariables(file,'NEW',{expectedDatabase:'banking_modal_v2_test'})});
   results.push({file,sha256:digest(fs.readFileSync(path.join(repoRoot,file),'utf8').replaceAll('\r\n','\n')),status:'PASS',startedAt,endedAt:new Date().toISOString()});}
  catch(error){results.push({file,status:'FAILED',code:error.code??'LOCAL_VERIFIER_FAILED',sqlState:error.sqlState??null});throw error;}
 }
 const fixtureCounts=JSON.parse(psql({sql:"select json_build_object('users',(select count(*) from public.tms_users where id::text like 'f7100000-%'),'clients',(select count(*) from public.clients where id::text like 'f7100000-%'),'groups',(select count(*) from public.weekly_source_groups where id::text like 'f7100000-%'),'memberships',(select count(*) from public.weekly_source_group_clients where id::text like 'f7100000-%'),'cycles',(select count(*) from public.weekly_source_cycles where id::text like 'f7100000-%'));"}));
 if(Object.values(fixtureCounts).some(value=>value!==0))throw Error('LOCAL_PP_FIXTURE_ROLLBACK_INCOMPLETE');
 const report={status:'LOCAL_MECHANICAL_SEAL_PP_FIRST_USE_AND_SECURITY_PASS',beganAt,endedAt:new Date().toISOString(),
  physicalDatabase:'banking_modal_v2_test',major:17,contractHash:canonicalContractHash(readJson('supabase/release/current-contract.json')),
  results,fixtureCounts,hostedExecution:false,fullUpgradeVerification:'NOT_RUN',financialApiDocumentQualification:'NOT_PROVED'};
 fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/LOCAL_PP_FIRST_USE_SECURITY.json'),JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify(report));
}catch(error){fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/LOCAL_PP_FIRST_USE_SECURITY_FAILED.json'),JSON.stringify({status:'LOCAL_SEAL_VERIFICATION_FAILED',beganAt,endedAt:new Date().toISOString(),results,
 code:error.code??'LOCAL_VERIFIER_FAILED',sqlState:error.sqlState??null,hostedExecution:false},null,2)+'\n');
 console.error(error.code??'LOCAL_SEAL_VERIFICATION_FAILED');process.exitCode=1;}
