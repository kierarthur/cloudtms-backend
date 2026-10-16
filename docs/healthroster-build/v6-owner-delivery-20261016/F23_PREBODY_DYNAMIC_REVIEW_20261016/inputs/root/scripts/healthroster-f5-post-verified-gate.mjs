// Proposed scripts/healthroster-f5-post-verified-gate.mjs. No connection or
// mutation at import. Root must mount this at both managed and automatic gates.
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {proveCommittedSources,assertExecutionTreeProof,sourceInventory} from './weekly-source-protected-execution-tree.mjs';
import {assertPostVerifiedReceipt} from './weekly-source-post-verified-evidence-controller.mjs';
import {generateF5SyntheticFixture,compileF5SyntheticVerifier,f5ParserInputs} from './healthroster-f5-synthetic-fixture.mjs';
export const f5Verifier='supabase/verification/13102026_1440_weekly_source_invoice_money_staging_hold_verification.sql';
// Original AUTO authority roots include all migrations/repeatables/includes,
// contracts/manifests/verifiers and scripts. Include the actual pure builder's
// proxy dependency as an additional gate-authority input, never a skip flag.
export const f5AuthorityPaths=Object.freeze(['supabase','scripts','.github/workflows/database-release.yml',
 '.github/workflows/automatic-test-release.yml','package.json','package-lock.json','broker/src/weekly-source/off-rule-native-proxy.mjs',...f5ParserInputs]);
const fail=code=>{throw Error(code);};
export function assertF5AuthorityCompatibility(root,databaseCommit,publicationCommit,registrations=[],execution=null){
 if(typeof root!=='string'||![databaseCommit,publicationCommit].every(value=>/^[0-9a-f]{40}$/.test(value)))fail('F5_DATABASE_SOURCE_AUTHORITY_REQUIRED');
 const git=args=>execFileSync('git',['--no-replace-objects',...args],{cwd:root,stdio:['ignore','pipe','pipe'],maxBuffer:16*1024*1024});
 try{
  for(const commit of [databaseCommit,publicationCommit])git(['cat-file','-e',commit+'^{commit}']);
  const exactAuthority=(left,right)=>{
   const before=git(['ls-tree','-r','-z',left,'--',...f5AuthorityPaths]);
   const after=git(['ls-tree','-r','-z',right,'--',...f5AuthorityPaths]);
   return before.length&&before.equals(after)?createHash('sha256').update(before).digest('hex'):null;
  };
  const exact=exactAuthority(databaseCommit,publicationCommit);
  if(exact)return {sha256:exact,registration:null};
  // The sole normalization is the EXISTING protected owner's complete tracked
  // source proof and closed receipt slots, backed by its actual native audit.
  // No directory exclusion, caller compatibility flag or new evidence owner.
  if(!Array.isArray(registrations)||registrations.length>100||!execution
   ||execution.source_commit!==databaseCommit)fail('F5_NATIVE_EVIDENCE_REGISTRATION_REQUIRED');
  const candidates=[];
  for(const record of registrations){
   try{
    if(!record||JSON.stringify(Object.keys(record).sort())!==JSON.stringify(['artifacts','receipt'])
     ||!Array.isArray(record.artifacts)||record.artifacts.length<1||record.artifacts.length>100)continue;
    const r=record.receipt;
    if(!r||r.artifact_slot!=='weekly_source_protected_test_setup_authorizations'
     ||JSON.stringify(r.execution)!==JSON.stringify(execution)||r.receipt_input_commit===databaseCommit)continue;
    const suffix=exactAuthority(r.receipt_input_commit,publicationCommit);if(!suffix)continue;
    const proof=proveCommittedSources(root,databaseCommit,r.receipt_input_commit);
    assertExecutionTreeProof(proof,databaseCommit,r.receipt_input_commit);
    const manifest=JSON.parse(git(['show',r.receipt_input_commit+':'+sourceInventory.manifest]));
    const installedManifest=JSON.parse(git(['cat-file','blob',databaseCommit+':'+sourceInventory.manifest]));
    // The native CLI record is BASE-mode-installedCommitPrefix, not BASE.
    if(manifest.releaseId!==installedManifest.releaseId||!['NEW','UPGRADE'].some(mode=>
     execution.release_id===`${installedManifest.releaseId}-${mode.toLowerCase()}-${databaseCommit.slice(0,12)}`))continue;
    assertPostVerifiedReceipt(r,{expected_execution:execution,receipt_input_commit:r.receipt_input_commit,
     receipt_tree_proof:proof,artifact_slot:r.artifact_slot,artifacts:record.artifacts,
     expected_evidence_sha256:r.prior_evidence_sha256},r.actor_user_id);
    const hash=createHash('sha256').update(JSON.stringify({source_equivalence_sha256:proof.source_equivalence_sha256,
     receipt_sha256:r.receipt_sha256,publication_authority_sha256:suffix})).digest('hex');
    candidates.push({sha256:hash,registration:record});
   }catch{} // A rejected audit record confers no authority; never infer latest.
  }
  // Genuine recorder replay can append another valid audit receipt. Every
  // admitted receipt must prove the SAME source/artifact binding. A sorted
  // receipt hash selects a stable representation; recency confers no power.
  const bindings=new Set(candidates.map(c=>JSON.stringify([c.registration.receipt.receipt_input_commit,
   c.registration.receipt.receipt_tree_sha256,c.registration.receipt.artifacts_sha256])));
  if(!candidates.length||bindings.size!==1)fail('F5_ONE_NATIVE_EVIDENCE_REGISTRATION_REQUIRED');
  return candidates.sort((a,b)=>a.registration.receipt.receipt_sha256.localeCompare(b.registration.receipt.receipt_sha256))[0];
 }catch{fail('F5_DATABASE_SOURCE_AUTHORITY_CHANGED');}
}
export function partitionF5VerificationFiles(release,mode){
 if(!['NEW','UPGRADE'].includes(mode))fail('F5_RELEASE_MODE_REQUIRED');
 for(const key of ['verificationFiles','newVerificationFiles']){
  if(!Array.isArray(release[key])||release[key].filter(p=>p===f5Verifier).length!==1)fail('F5_MANDATORY_VERIFIER_REGISTRATION_REQUIRED');
 }
 const files=mode==='NEW'?release.newVerificationFiles:release.verificationFiles;
 return {preVerified:files.filter(p=>p!==f5Verifier),postVerified:[f5Verifier]};
}
export async function readF5VerifiedState({psql,expectedContractSha256,restorationSql}){
 if(typeof psql!=='function'||!/^[0-9a-f]{64}$/.test(expectedContractSha256)||typeof restorationSql!=='string'
  ||!restorationSql.includes('F5_ROLLBACK_RESTORATION_V1'))fail('F5_PROTECTED_GATE_INPUT_REQUIRED');
 const state=JSON.parse(await psql({sql:restorationSql}));
 if(state.database!=='cloudtms_test_clone'||state.execution?.release_id==null
  ||!/^[0-9a-f]{40}$/.test(state.execution?.source_commit)||state.execution?.contract_sha256!==expectedContractSha256
  ||!/^[0-9a-f]{64}$/.test(state.restoration_sha256)||state.fixture_vacant!==true
  ||!Array.isArray(state.authority_registrations)||state.authority_registrations.length>100)fail('F5_ACTUAL_VERIFIED_EXECUTION_REQUIRED');
 return state;
}
export async function runF5PostVerifiedGate({psql,root,sourceCommit,expectedContractSha256,restorationSql}){
 if(typeof psql!=='function'||!/^[0-9a-f]{40}$/.test(sourceCommit)||!/^[0-9a-f]{64}$/.test(expectedContractSha256)
  ||typeof restorationSql!=='string'||!restorationSql.includes('F5_ROLLBACK_RESTORATION_V1'))fail('F5_PROTECTED_GATE_INPUT_REQUIRED');
 const inspect=()=>readF5VerifiedState({psql,expectedContractSha256,restorationSql});
 const before=await inspect();
 const authority=assertF5AuthorityCompatibility(root,before.execution.source_commit,sourceCommit,before.authority_registrations,before.execution);
 // A fresh synthetic file is generated through actual parser/adapter owners
 // for this real past open window. No old upload/clock is relabelled. The
 // committed mandatory verifier supplies every assertion and transaction.
 const generated=await generateF5SyntheticFixture(before.f5_fixture_context);
 const compiled=compileF5SyntheticVerifier({root,sourceCommit,verifier:f5Verifier,generated});
 await psql({sql:compiled.sql});
 const after=await inspect(); // psql's next invocation is a fresh connection.
 const stable=state=>({...state,f5_fixture_context:state.f5_fixture_context?{...state.f5_fixture_context,observed_at_utc:null}:null});
 if(JSON.stringify(stable(after))!==JSON.stringify(stable(before)))fail('F5_FRESH_ROLLBACK_RESTORATION_FAILED');
 return {contract:'F5_POST_VERIFIED_ROLLBACK_GATE_V1',database:'cloudtms_test_clone',release_id:before.execution.release_id,
  database_source_commit:before.execution.source_commit,database_authority_sha256:authority.sha256,
  authority_registration:authority.registration,
  registered_verifier_sha256:compiled.registered_verifier_sha256,synthetic_fixture:generated.proof,
  source_commit:sourceCommit,contract_sha256:expectedContractSha256,restoration_sha256:after.restoration_sha256,
  qualification:'ROLLBACK_SQL_PASS',features_remain_off:true,provider_or_runtime_acceptance:false};
}
