import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {isDeepStrictEqual} from 'node:util';
import {repoRoot, openManagedPsqlSession, validateTarget, validateExpectedDatabase} from './cloudtms-db-release-lib.mjs';
import {exact} from './weekly-source-protected-release-qualification.mjs';
import {preparePostVerifiedRequest, assertPostVerifiedReceipt} from './weekly-source-post-verified-evidence-controller.mjs';

const fail=code=>{throw Error(code);}, commit=/^[0-9a-f]{40}$/,
 uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
export const recordSlots=Object.freeze(['weekly_source_protected_test_setup_authorizations','weekly_source_off_rule_test_controls',
 'weekly_source_protected_partial_native_evidence','weekly_source_protected_activation_probe_authorizations',
 'weekly_source_protected_capability_qualifications','weekly_source_protected_prerequisite_qualifications',
 'weekly_source_report_fin_native_comparisons','weekly_source_report_fin_qualifications']);
const reportSlots=recordSlots.slice(-2), ownSource='scripts/weekly-source-protected-record-evidence.mjs';
export const ownerInitializeSql=`begin;
 set local lock_timeout='5s'; set local statement_timeout='120s';
 do $protected_owner$ declare actual_owner name; begin
  select pg_get_userbyid(proowner) into strict actual_owner from pg_proc
   where oid='private.weekly_source_protected_qualification_owner_v1()'::regprocedure;
  if current_database()<>'cloudtms_test_clone' or session_user is distinct from actual_owner
   or current_user is distinct from actual_owner or not exists(select 1 from private.cloudtms_database_identity
    where singleton and environment='TEST') then
   raise exception 'PROTECTED_RECORD_TEST_OWNER_REQUIRED' using errcode='42501'; end if;
 end $protected_owner$;
 -- This is the existing protected owner request dialect, after actual login
 -- ownership has been proved. It grants no database role or actor identity.
 select set_config('request.jwt.claim.role','service_role',false);
 select private.weekly_source_protected_qualification_owner_v1();
 commit;`;
export const installedStateSql=`select jsonb_build_object(
 'execution',private.weekly_source_protected_qualification_release_v1(),
 'release_identity',to_jsonb(r)-'evidence_json',
 'database_identity',(select to_jsonb(i) from private.cloudtms_database_identity i where singleton),
 'migration_ledger',(select coalesce(jsonb_agg(to_jsonb(m) order by to_jsonb(m)::text),'[]'::jsonb) from private.cloudtms_migration_ledger m),
 'repeatable_ledger',(select coalesce(jsonb_agg(to_jsonb(p) order by to_jsonb(p)::text),'[]'::jsonb) from private.cloudtms_repeatable_ledger p),
 'installed_catalogue',private.weekly_source_protected_qualification_current_catalogue_v1(),
 'evidence_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_RELEASE_EVIDENCE_V1',r.evidence_json),'hex'))::text
 from private.cloudtms_database_releases r order by started_at_utc desc,release_id desc limit 1;`;
const literal=value=>"'"+String(value).replaceAll("'","''")+"'";
const git=(root,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:32*1024*1024});
export function protectedInputs(env){
 const receiptInputCommit=env.CLOUDTMS_RECORD_INPUT_COMMIT, actorUserId=env.CLOUDTMS_RECORD_ACTOR_USER_ID,
  artifactSlot=env.CLOUDTMS_RECORD_ARTIFACT_SLOT, reason=env.CLOUDTMS_RECORD_REASON;
 if(env.GITHUB_ACTIONS!=='true'||env.GITHUB_REPOSITORY!=='kierarthur/cloudtms-backend'
  ||env.CLOUDTMS_PROTECTED_ENVIRONMENT!=='database-test'||env.CLOUDTMS_ENVIRONMENT!=='TEST'
  ||env.CLOUDTMS_EXPECTED_DATABASE!=='cloudtms_test_clone'||!env.CLOUDTMS_EXPECTED_TARGET
  ||!/^refs\/heads\/.+/.test(env.GITHUB_REF||'')||!commit.test(receiptInputCommit||'')
  ||env.GITHUB_SHA!==receiptInputCommit||!uuid.test(actorUserId||'')||!recordSlots.includes(artifactSlot)
  ||typeof reason!=='string'||reason.trim()!==reason||reason.length<1||reason.length>1000
  ||env.CLOUDTMS_RELEASE_REQUESTED_APPROVAL!==`RECORD_EVIDENCE TEST ${receiptInputCommit} ${artifactSlot} ${actorUserId}`)
  fail('PROTECTED_RECORD_EXACT_TEST_WORKFLOW_INPUT_REQUIRED');
 return {receiptInputCommit,actorUserId,artifactSlot,reason};
}
export function assertCheckout(root,receiptInputCommit){
 if(git(root,['rev-parse','HEAD']).toString('ascii').trim()!==receiptInputCommit
  ||git(root,['status','--porcelain=v1','--untracked-files=normal']).length)
  fail('PROTECTED_RECORD_CLEAN_EXACT_CHECKOUT_REQUIRED');
}
export function assertInstalledEntry(root,execution,entryBytes){
 if(!commit.test(execution.source_commit||'')||!Buffer.isBuffer(entryBytes)
  ||!git(root,['show',execution.source_commit+':'+ownSource]).equals(entryBytes))
  fail('PROTECTED_RECORD_ENTRY_NOT_INSTALLED');
}
function state(output){
 let value;try{value=JSON.parse(output);}catch{fail('PROTECTED_RECORD_NATIVE_STATE_INVALID');}
 exact(value,['execution','release_identity','database_identity','migration_ledger','repeatable_ledger','installed_catalogue','evidence_sha256'],
  'PROTECTED_RECORD_NATIVE_STATE_INVALID');
 if(value.release_identity?.status!=='VERIFIED'||!value.release_identity.completed_at_utc
  ||value.release_identity.git_commit!==value.execution?.source_commit
  ||value.release_identity.installed_contract_sha256!==value.execution?.contract_sha256
  ||value.release_identity.repository_contract_sha256!==value.execution?.contract_sha256
  ||value.execution?.environment!=='TEST'||value.execution?.database!=='cloudtms_test_clone'
  ||value.database_identity?.installation_id!==value.execution?.installation_id
  ||value.database_identity?.environment!=='TEST'||!Array.isArray(value.migration_ledger)
  ||!Array.isArray(value.repeatable_ledger)||!value.installed_catalogue
  ||!/^[0-9a-f]{64}$/.test(value.evidence_sha256||''))fail('PROTECTED_RECORD_NATIVE_STATE_INVALID');
 return value;
}
export function assertUnchangedInstalledState(before,after,receipt){
 const {evidence_sha256:beforeEvidence,...beforeNative}=before,{evidence_sha256:afterEvidence,...afterNative}=after;
 if(!isDeepStrictEqual(beforeNative,afterNative)||afterEvidence!==receipt.evidence_sha256
  ||beforeEvidence!==receipt.prior_evidence_sha256)fail('PROTECTED_RECORD_NATIVE_STATE_CHANGED');
}
const backendPid=output=>{const value=Number(output.trim());if(!/^[1-9][0-9]*$/.test(output.trim())
 ||!Number.isSafeInteger(value)||value>2147483647)fail('PROTECTED_RECORD_ACTUAL_PID_REQUIRED');return value;};

// Fixed transport operation for the trusted workflow. Session injection exists
// for effect-free tests and the root-owned runner; no HTTP/public SQL entry or
// arbitrary artifact constructor is exposed. Production main owns both actual
// connections and reads all identities through the native installed reader.
export async function recordInstalledEvidence({root,inputs,session,turnSession,entryBytes}){
 if(!session||!turnSession||session===turnSession||typeof session.execute!=='function'||typeof turnSession.execute!=='function'
  ||typeof session.close!=='function'||typeof turnSession.close!=='function')fail('PROTECTED_RECORD_TWO_OWNER_SESSIONS_REQUIRED');
 let turnHeld=false,writerHeld=false,begun=false,committed=false;
 try{
  await turnSession.execute(ownerInitializeSql);await session.execute(ownerInitializeSql);
  const turnBackendPid=backendPid(await turnSession.execute('select pg_backend_pid();'));
  const recordBackendPid=backendPid(await session.execute('select pg_backend_pid();'));
  if(turnBackendPid===recordBackendPid)fail('PROTECTED_RECORD_SEPARATE_TURN_REQUIRED');
  if((await turnSession.execute('select pg_try_advisory_lock(20261013,1);')).trim()!=='t')fail('PROTECTED_RECORD_SEND7_TURN_BUSY');turnHeld=true;
  if((await session.execute("select pg_try_advisory_lock(hashtextextended('cloudtms_database_release_admission_v1',0));")).trim()!=='t')
   fail('PROTECTED_RECORD_MANAGED_WRITER_BUSY');writerHeld=true;
  await session.execute("begin; set local lock_timeout='5s'; set local statement_timeout='120s'; select private.weekly_source_protected_qualification_owner_v1();");begun=true;
  // Actor is the exact approved Office UUID, independently checked by the
  // actual native admin owner, not synthesized from the GitHub login or PID.
  await session.execute('select private._weekly_source_settings_require_admin_v1('+literal(inputs.actorUserId)+'::uuid);');
  const before=state(await session.execute(installedStateSql));
  assertCheckout(root,inputs.receiptInputCommit);assertInstalledEntry(root,before.execution,entryBytes);
  let request,assertReceipt;
  if(reportSlots.includes(inputs.artifactSlot)){
   const report=await import('./weekly-source-report-fin-ab-evidence.mjs');
   request=await report.preparePostVerifiedReportRequest({root,expectedExecution:before.execution,actorUserId:inputs.actorUserId,
    expectedEvidenceSha256:before.evidence_sha256,receiptInputCommit:inputs.receiptInputCommit,artifactSlot:inputs.artifactSlot,
    reason:inputs.reason,turnBackendPid,execute:sql=>session.execute(sql),literal});
   assertReceipt=report.assertPostVerifiedReportReceipt;
  }else{
   request=preparePostVerifiedRequest({root,execution:before.execution,actorUserId:inputs.actorUserId,
    expectedEvidenceSha256:before.evidence_sha256,receiptInputCommit:inputs.receiptInputCommit,artifactSlot:inputs.artifactSlot,
    reason:inputs.reason,turnBackendPid});assertReceipt=assertPostVerifiedReceipt;
  }
  const receipt=JSON.parse(await session.execute('select private.weekly_source_post_verified_evidence_record_v1('
   +literal(inputs.actorUserId)+'::uuid,'+literal(JSON.stringify(request))+'::jsonb)::text;'));
  assertReceipt(receipt,request,inputs.actorUserId);
  assertUnchangedInstalledState(before,state(await session.execute(installedStateSql)),receipt);
  await session.execute('commit;');committed=true;
  // Retain the actual managed session/mutex for fresh READ COMMITTED readback.
  // An uncertain commit/readback is never retried or described as rolled back.
  assertUnchangedInstalledState(before,state(await session.execute(installedStateSql)),receipt);
  return receipt;
 }finally{
  if(begun&&!committed)try{await session.execute('rollback;');}catch{}
  if(writerHeld)try{await session.execute("select pg_advisory_unlock(hashtextextended('cloudtms_database_release_admission_v1',0));");}catch{}
  if(turnHeld)try{await turnSession.execute('select pg_advisory_unlock(20261013,1);');}catch{}
  const closed=await Promise.allSettled([session.close(),turnSession.close()]);
  if(closed.some(result=>result.status==='rejected'))fail('PROTECTED_RECORD_OWNER_SESSION_CLOSE_FAILED');
 }
}
export async function main(){
 if(process.argv.length!==3||process.argv[2]!=='record')fail('PROTECTED_RECORD_FINITE_OPERATION_REQUIRED');
 const inputs=protectedInputs(process.env);assertCheckout(repoRoot,inputs.receiptInputCommit);
 // Use the existing protected TEST secret only. No local route, account token,
 // role substitution, connection-string override or installer is admitted.
 const target=new URL(process.env.CLOUDTMS_DATABASE_URL||'');
 if(!['postgres:','postgresql:'].includes(target.protocol)||['localhost','127.0.0.1','::1','[::1]','host.docker.internal'].includes(target.hostname)
  ||process.env.CLOUDTMS_ALLOW_LOCAL)fail('PROTECTED_RECORD_HOSTED_TEST_TARGET_REQUIRED');
 validateTarget('TEST',process.env.CLOUDTMS_EXPECTED_TARGET);validateExpectedDatabase('cloudtms_test_clone');
 let session,turnSession;
 try{
  turnSession=openManagedPsqlSession();session=openManagedPsqlSession();
  const receipt=await recordInstalledEvidence({root:repoRoot,inputs,session,turnSession,entryBytes:fs.readFileSync(fileURLToPath(import.meta.url))});
  const output=path.join(repoRoot,'.codex-tmp','weekly-source-post-verified-record-receipt.json');
  fs.mkdirSync(path.dirname(output),{recursive:true});fs.writeFileSync(output,JSON.stringify(receipt,null,2)+'\n',{mode:0o600});
  console.log('Protected TEST same-installed evidence recorded; exact receipt saved.');
 }finally{
  // Also closes a first connection if opening the second one failed.
  await Promise.allSettled([session?.close(),turnSession?.close()]);
 }
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))main().catch(error=>{
 // Managed SQL errors are already scrubbed; never print input, SQL, URL or
 // server DETAIL/CONTEXT. The protected runner preserves the finite failure.
 const safeCode=/^(?:PROTECTED_RECORD|POST_VERIFIED|REPORT_AB|QUALIFICATION)_[A-Z0-9_]{1,140}$/.test(error?.message||'')
  ?error.message:'PROTECTED_RECORD_OWNER_COMMAND_FAILED';
 console.error(safeCode+'; no automatic retry or rollback claim.');process.exitCode=1;
});
