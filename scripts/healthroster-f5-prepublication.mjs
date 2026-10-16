// Proposed desktop coordinator bridge. Always validates the protected AUTO
// artifact after database success and before the first Worker publication.
import fs from 'node:fs';
import path from 'node:path';
import {assertF5AuthorityCompatibility} from './healthroster-f5-post-verified-gate.mjs';
import {assertF5SyntheticProof} from './healthroster-f5-synthetic-fixture.mjs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {f5Verifier} from './healthroster-f5-post-verified-gate.mjs';
const fail=code=>{throw Error(code);};
const hex=/^[0-9a-f]{64}$/;
export function validateF5PrepublicationReceipt({root,run,automaticReceipt,expectedCommit,expectedContractSha256}){
 if(!/^[0-9a-f]{40}$/.test(expectedCommit)||!hex.test(expectedContractSha256)
  ||!Number.isSafeInteger(run?.id)||run.id<1||run.head_sha!==expectedCommit||run.event!=='workflow_dispatch'
  ||run.status!=='completed'||run.conclusion!=='success')fail('F5_PROTECTED_AUTO_RUN_REQUIRED');
 const value=automaticReceipt;
 if(value?.formatVersion!==1||value.environment!=='TEST'||value.database!=='cloudtms_test_clone'
  ||value.status!=='DATABASE_READY'||value.commit!==expectedCommit||!Number.isFinite(Date.parse(value.completedAt)))fail('F5_PROTECTED_AUTO_RECEIPT_REQUIRED');
 const gate=value.f5Gate;
 const fields=['contract','database','release_id','database_source_commit','database_authority_sha256','authority_registration','registered_verifier_sha256','synthetic_fixture','source_commit','contract_sha256','restoration_sha256','qualification','features_remain_off','provider_or_runtime_acceptance'].sort();
 if(!gate||typeof gate!=='object'||Array.isArray(gate)||JSON.stringify(Object.keys(gate).sort())!==JSON.stringify(fields)
  ||gate.contract!=='F5_POST_VERIFIED_ROLLBACK_GATE_V1'||gate.database!=='cloudtms_test_clone'
  ||typeof gate.release_id!=='string'||gate.release_id.length<1||gate.release_id.length>300
  ||gate.source_commit!==expectedCommit||gate.contract_sha256!==expectedContractSha256
  ||!/^[0-9a-f]{40}$/.test(gate.database_source_commit)||!hex.test(gate.database_authority_sha256)
  ||!hex.test(gate.restoration_sha256)||gate.qualification!=='ROLLBACK_SQL_PASS'
  ||gate.features_remain_off!==true||gate.provider_or_runtime_acceptance!==false)fail('F5_NATIVE_ROLLBACK_RECEIPT_REQUIRED');
 if(value.databaseRelease?.release_id!==gate.release_id||value.databaseRelease?.source_commit!==gate.database_source_commit
  ||value.databaseRelease?.contract_sha256!==gate.contract_sha256)fail('F5_ACTUAL_DATABASE_RELEASE_RECEIPT_REQUIRED');
 const registration=gate.authority_registration;
 if(registration!==null&&(!registration||registration.receipt?.execution?.release_id!==gate.release_id
  ||registration.receipt?.execution?.source_commit!==gate.database_source_commit
  ||registration.receipt?.execution?.contract_sha256!==gate.contract_sha256))fail('F5_NATIVE_EVIDENCE_REGISTRATION_REQUIRED');
 const authority=assertF5AuthorityCompatibility(root,gate.database_source_commit,expectedCommit,
  registration===null?[]:[registration],registration?.receipt.execution);
 if(authority.sha256!==gate.database_authority_sha256||JSON.stringify(authority.registration)!==JSON.stringify(registration))fail('F5_DATABASE_AUTHORITY_RECEIPT_MISMATCH');
 assertF5SyntheticProof(gate.synthetic_fixture);
 const verifierBytes=execFileSync('git',['--no-replace-objects','cat-file','blob',expectedCommit+':'+f5Verifier],{cwd:root,maxBuffer:2*1024*1024,stdio:['ignore','pipe','pipe']});
 if(createHash('sha256').update(verifierBytes).digest('hex')!==gate.registered_verifier_sha256)fail('F5_REGISTERED_VERIFIER_SOURCE_MISMATCH');
 // The receipt is accepted only after the fixed protected run artifact loader
 // below ties it to the selected successful AUTO workflow and exact source.
 return {...gate,protected_run_id:run.id};
}
export async function loadF5PrepublicationReceipt({root,command,api,gh,run,expectedCommit,expectedContractSha256}){
 if(typeof command!=='function'||typeof api!=='function'||typeof gh!=='string'||!gh.length||!Number.isSafeInteger(run?.id)||run.id<1)fail('F5_PROTECTED_ARTIFACT_LOADER_REQUIRED');
 const name='automatic-test-release-'+expectedCommit;
 const listing=await api(`repos/kierarthur/cloudtms-backend/actions/runs/${run.id}/artifacts?per_page=100`);
 const matches=listing?.artifacts?.filter(item=>item.name===name&&item.expired===false);
 if(!Number.isSafeInteger(listing?.total_count)||listing.total_count>100||!Array.isArray(matches)||matches.length!==1||!Number.isSafeInteger(matches[0].id)||matches[0].id<1)fail('F5_ONE_CURRENT_AUTO_ARTIFACT_REQUIRED');
 const base=path.resolve(root,'.codex-tmp');
 fs.mkdirSync(base,{recursive:true});
 if(fs.lstatSync(base).isSymbolicLink())fail('F5_ARTIFACT_DIRECTORY_LINK_REFUSED');
 const destination=fs.mkdtempSync(path.join(base,'f5-prepublication-'+run.id+'-'));
 if(path.dirname(destination)!==base)fail('F5_FRESH_ARTIFACT_DIRECTORY_REQUIRED');
 // Reuse the existing guarded coordinator command and GitHub authentication.
 // No deployment/SQL/new sender/credential route is opened by this loader.
 await command(gh,['run','download',String(run.id),'--repo','kierarthur/cloudtms-backend','--name',name,'--dir',destination]);
 const file=path.join(destination,'automatic-test-release.json');
 const stat=fs.lstatSync(file);
 if(!stat.isFile()||stat.isSymbolicLink()||stat.size<2||stat.size>1048576)fail('F5_BOUNDED_REGULAR_AUTO_RECEIPT_REQUIRED');
 const automaticReceipt=JSON.parse(fs.readFileSync(file,'utf8'));
 const receipt=validateF5PrepublicationReceipt({root,run,automaticReceipt,expectedCommit,expectedContractSha256});
 return {...receipt,protected_artifact_id:matches[0].id};
}
