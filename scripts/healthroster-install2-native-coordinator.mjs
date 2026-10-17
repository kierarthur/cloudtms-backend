// Owner-authorised Install2 only; additive entry, original Install1 remains unchanged. Importing this module opens no connection.
// SQL credentials remain in the approved child environment, never in argv/artifacts.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {withSharedRehearsalTurn,agencyChildEnvironment,assertNoProtectedRelease} from './healthroster-rehearsal-turn.mjs';
import {abortableDelay} from './healthroster-turn-control.mjs';
import {runFullRehearsalWithinOwnedTurn,executePsqlWithinOwnedTurn,restorationSnapshotSql,identityProbeSql,rehearsalSourceBinding,assertRehearsalSourceBinding,expectedContractDigestSql} from './healthroster-install1-full-rehearsal.mjs';
import {validateConnectionProof} from './automatic-test-release-policy.mjs';
import {canonical,digest,scanManagedSql} from './cloudtms-managed-release-sql.mjs';
import {createReceiptJournal,latestBusyTimestamp} from './healthroster-receipt-journal.mjs';
import {freshFixtureProofSql,qualifyLiveRestoration} from './healthroster-live-test-restoration.mjs';
import assert from 'node:assert/strict';
import {canonicalContractHash,inventory} from './cloudtms-db-release-lib.mjs';
import {assertReview} from './healthroster-install1-full-rehearsal.mjs';
import {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';
import {collectInstall2ReviewedInstalledSource} from './healthroster-install2-installed-source.mjs';
import {collectInstall2SourceRelationship} from './healthroster-install2-source-relationship.mjs';
import {createRehearsalStreamObserver} from './healthroster-rehearsal-stream-observer.mjs';
import {createRehearsalResourceGuard,createMigetRehearsalResourceReader} from './healthroster-rehearsal-resource-guard.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const workspace='D:/Development/GitHub';
const completedBankSource='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';
const configFile='docs/healthroster-build/install2-rehearsal/NATIVE_COORDINATOR_CONFIG.json';
const psqlPath='C:/Program Files/PostgreSQL/18/bin/psql.exe';
const gh='C:/Program Files/GitHub CLI/gh.exe';
const out=path.join(root,'.codex-tmp/healthroster-install2-receipts');
const progressPath=path.join(root,'docs/healthroster-build/PROGRESS.md');
const targets=[['backend','test-cloudtms-backend','deploy/cloudflare/test-cloudtms-backend'],['private','test-cloudtms-candidate-private-api','deploy/cloudflare/test-candidate-private-api'],['synthetic','test-cloudtms-candidate-synthetic-private-api','deploy/cloudflare/test-candidate-synthetic-private-api'],['broker','test-cloudtms-candidate-broker','deploy/cloudflare/test-candidate-broker']];
const requireThat=(condition,code)=>{if(!condition)throw Error(code);};
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,encoding:'utf8',windowsHide:true,maxBuffer:16000000}).trim();
const remote=branch=>git(['ls-remote','origin','refs/heads/'+branch]).split(/\s/)[0];
const publicSnapshot=value=>{const {backgroundRows,backgroundSnapshotXip,...safe}=value;return safe;};
const publicExecution=value=>{if(!value)return value;const {fixtureObservations,...safe}=value;return {...safe,fixtureObservationCount:fixtureObservations?.length??0};};


export const INSTALL2_CONFIG_VERSION='HEALTHROSTER_INSTALL2_NATIVE_COORDINATOR_CONFIG_V1';
export const originalSafetyExclusions=Object.freeze(["tests/29082026_0117_candidate_daily_projection_expired_lease_recovery_runtime_verification.sql", "tests/31082026_1437_banking_pay_candidate_group_pagination_runtime_verification.sql", "tests/31082026_1907_banking_pay_candidate_group_display_only_selection_tuple_runtime_verification.sql", "tests/01092026_1511_banking_pay_signed_recovery_draft_runtime_verification.sql", "tests/02092026_2392_banking_pay_workbench_settled_certificate_v8_runtime_verification.sql", "supabase/verification/06092026_2358_candidate_advanced_expense_policy_verification.sql", "supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql", "supabase/verification/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql", "supabase/verification/15092026_1534_weekly_source_correct_final_source_v1.sql", "supabase/verification/15092026_1534_weekly_source_invoice_admission_v1.sql", "supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql", "supabase/verification/02092026_1833_weekly_source_invoice_issue_validator_v1.sql", "supabase/verification/17092026_0700_weekly_source_pending_entitlement_release_v1.sql", "supabase/verification/17092026_1100_weekly_source_candidate_view_producer_v1.sql", "supabase/verification/17092026_1200_weekly_source_audit_and_export_v1.sql", "supabase/verification/19092026_1645_weekly_source_completed_pack_copy_v1.sql"]);
export function validateInstall2Configuration(c){
 const keys=['version','installedSourceCommit','expectedRemoteTestPin','expectedContractHash','reviewedInstalledToRemotePaths','officePath'];
 requireThat(c&&typeof c==='object'&&!Array.isArray(c)&&JSON.stringify(Object.keys(c).sort())===JSON.stringify(keys.sort())
  &&c.version===INSTALL2_CONFIG_VERSION&&[c.installedSourceCommit,c.expectedRemoteTestPin].every(x=>/^[a-f0-9]{40}$/.test(x??''))
  &&/^[a-f0-9]{64}$/.test(c.expectedContractHash??''),'INSTALL2_CLOSED_CONFIGURATION_REQUIRED');
 requireThat(typeof c.officePath==='string'&&/^D:\/Development\/GitHub\/\.codex-worktrees\/[a-z0-9-]+$/.test(c.officePath),
  'INSTALL2_REVIEWED_D_OFFICE_PATH_REQUIRED');
 requireThat(Array.isArray(c.reviewedInstalledToRemotePaths)&&c.reviewedInstalledToRemotePaths.length<=100
  &&new Set(c.reviewedInstalledToRemotePaths.map(r=>r?.path)).size===c.reviewedInstalledToRemotePaths.length
  &&c.reviewedInstalledToRemotePaths.every(r=>r&&JSON.stringify(Object.keys(r).sort())===JSON.stringify(['path','beforeContentSha256','afterContentSha256','decision'].sort())
   &&/^(?:docs\/healthroster-build\/(?:PUBLISHED_HELPERS\.md|TEST_INSTALLS\.md|install1-completed-20261014\/[A-Za-z0-9_.-]+))$/.test(r.path)
   &&r.decision==='INSTALL1_PUBLICATION_RECEIPT_ONLY'&&(r.beforeContentSha256===null||/^[a-f0-9]{64}$/.test(r.beforeContentSha256??''))
   &&/^[a-f0-9]{64}$/.test(r.afterContentSha256??'')),'INSTALL2_EXACT_RECEIPT_GAP_REVIEW_REQUIRED');
 return c;
}
export function validateInstall2Review(review,release,current){
 assertReview(review,release); // Shared original review schema, not a fabricated Install2 schema.
 requireThat(review.sourceCounts?.migrations===current.migrations.length&&review.sourceCounts?.repeatables===current.repeatables.length
  &&review.sourceCounts?.verifiers===release.verificationFiles.length&&review.verifiers.every(r=>r.pendingIndependentReview!==true),
  'INSTALL2_CURRENT_COMPLETE_REVIEW_REQUIRED');
 const excluded=review.verifiers.filter(r=>r.decision==='EXCLUDE').map(r=>r.file).sort();
 requireThat(JSON.stringify(excluded)===JSON.stringify([...originalSafetyExclusions].sort()),'INSTALL2_ORIGINAL_SIXTEEN_EXCLUSIONS_REQUIRED');
 return review;
}
// One fresh RO/RR child returns identity, exact complete ledgers and the canonical
// exporter result from one snapshot. No caller-supplied SQL or catalogue adoption.
export function install2PredecessorProbeSql(marker){
 requireThat(arguments.length===1,'INSTALL2_FIXED_PREDECESSOR_ARGUMENT_REQUIRED');
 const source=canonical(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'));
 const parsed=scanManagedSql(source);
 requireThat(parsed.statements.length===2&&parsed.statements[0].tokens.join(' ')==='SET SEARCH_PATH = PG_CATALOG , PUBLIC'
  &&parsed.statements[1].tokens[0]==='WITH','INSTALL2_FIXED_EXPORTER_QUERY_REQUIRED');
 const query=source.slice(parsed.statements[1].start,parsed.statements[1].end).trim().replace(/;\s*$/,'');
 const identity=identityProbeSql(marker).trim().replace(/;\s*$/,'');
 return `set local search_path=pg_catalog,public;
 with native_identity(value) as (${identity}), native_contract(value) as (${query})
 select jsonb_build_object('serverVersionNum',current_setting('server_version_num')::integer,'identity',(select value::jsonb from native_identity),'contract',(select value::jsonb from native_contract),
 'installed',jsonb_build_object('migrations',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256) order by path),'[]'::jsonb) from private.cloudtms_migration_ledger),
 'repeatables',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger)));`;
}

export function parseOneJsonResult(stdout){
 const lines=stdout.split(/\r?\n/).filter(x=>x.trim().startsWith('{')||x.trim().startsWith('['));
 requireThat(lines.length===1,'NATIVE_PROBE_JSON_ENVELOPE_REFUSED');
 try{return JSON.parse(lines[0]);}catch{throw Error('NATIVE_PROBE_JSON_INVALID');}
}

function completedBankRecord(){
 const file=path.join(workspace,'.codex-worktrees/bpay-next-build-backend-20261009/docs/bpay-next/TEST_INSTALLS.md');
 const text=fs.readFileSync(file,'utf8');
 requireThat(text.includes('First disabled-first install — COMPLETE')&&text.includes(completedBankSource)
  &&text.includes('pg_advisory_unlock returned true')&&text.includes('2026-10-13T23:45:36.217Z'),'BANK_COMPLETED_RELEASE_RECORD_REQUIRED');
 const run=JSON.parse(execFileSync(gh,['api','repos/kierarthur/cloudtms-backend/actions/runs/38476430690'],{encoding:'utf8',windowsHide:true}));
 requireThat(run.status==='completed'&&run.conclusion==='success'&&run.head_sha===completedBankSource,'BANK_PROTECTED_COMPLETION_CHANGED');
 return {status:'COMPLETE',protectedConclusion:'success',sourcePin:completedBankSource,sharedUnlockConfirmed:true,recordHash:digest(text)};
}

export async function runNativeInstall2(){
 requireThat(process.argv[2]==='--run-owner-authorised-install2'&&process.argv.length===3,'EXPLICIT_OWNER_INSTALL2_ENTRY_REQUIRED');
 requireThat(fs.realpathSync.native(root).replaceAll('\\','/')==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026','INSTALL2_PHYSICAL_D_ROOT_REQUIRED');
 const config=validateInstall2Configuration(JSON.parse(fs.readFileSync(path.join(root,configFile),'utf8')));
 requireThat(fs.realpathSync.native(config.officePath).replaceAll('\\','/')===config.officePath,'INSTALL2_PHYSICAL_D_OFFICE_REQUIRED');
 const predecessor=config.expectedRemoteTestPin,contract=config.expectedContractHash;
 requireThat(git(['branch','--show-current'])==='codex/healthroster-install2-14102026','INSTALL2_OWN_BRANCH_REQUIRED');
 requireThat(!git(['status','--porcelain']),'INSTALL2_CLEAN_PUSHED_SOURCE_REQUIRED');
 const candidatePin=git(['rev-parse','HEAD']);
 requireThat(remote('codex/healthroster-install2-14102026')===candidatePin&&remote('test')===predecessor,'INSTALL2_SOURCE_REMOTE_CHANGED');
 const archiveProof=JSON.parse(fs.readFileSync(path.join(root,'.codex-tmp/healthroster-install2-archive-preflight.json'),'utf8'));
 requireThat(archiveProof.status==='SOURCE_PREFLIGHT_PASS'&&archiveProof.installNumber===2&&archiveProof.predecessor===predecessor&&archiveProof.commit===candidatePin&&archiveProof.linearAncestryPassed===true
  &&archiveProof.source==='EXACT_COMMITTED_GIT_ARCHIVE'&&archiveProof.sqlCarriageReturnFiles?.length===0
  &&archiveProof.steps?.length===9&&archiveProof.steps.every(row=>row.exitCode===0)&&archiveProof.cleanup?.removed===true
  &&archiveProof.dependencyProvisioning?.exitCode===0&&archiveProof.dependencyProvisioning.installerHooks===false
  &&archiveProof.dependencyProvisioning.lockUnchanged===true
  &&archiveProof.dependencyProvisioning.packageLockSha256===digest(canonical(fs.readFileSync(path.join(root,'package-lock.json'),'utf8'))),
  'INSTALL2_COMMITTED_ARCHIVE_PREFLIGHT_REQUIRED');
 requireThat(canonicalContractHash(JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8')))===contract,'INSTALL2_SEALED_CONTRACT_CHANGED');
 process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
 const review=JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/install2-rehearsal/VERIFIER_SAFETY_REVIEW.json'),'utf8'));
 validateInstall2Review(review,JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'),'utf8')),inventory());
 const reviewedInstalledSource=collectInstall2ReviewedInstalledSource({repositoryRoot:root,installedSourceCommit:config.installedSourceCommit});
 const sourceBinding=rehearsalSourceBinding({review});
 assertRehearsalSourceBinding(sourceBinding,{review,expectedContractHash:contract});
 const marker='hr_install2_'+crypto.randomBytes(16).toString('hex');
 const stamp=new Date().toISOString().replace(/[-:]/g,'').replace(/\.\d+Z$/,'Z');
 fs.mkdirSync(path.join(root,'.codex-tmp'),{recursive:true});
 const liveDirectory=fs.mkdtempSync(path.join(root,'.codex-tmp/healthroster-install2-native-'+stamp+'-'));
 const journal=createReceiptJournal(liveDirectory);
 const turnJournal=path.join(root,'.codex-tmp/healthroster-install2-turn-journal-20261017');
 fs.mkdirSync(turnJournal,{recursive:true});
 let receiptFile;
 const receipt={status:'PREPARING_NOT_RUN',candidatePin,predecessor,contract,marker,beganAt:new Date().toISOString(),errors:[],publication:null};
 const save=({continueRestoration=false}={})=>{
  try{receiptFile=journal.checkpoint(receipt);}
  catch(error){receipt.receiptPersistenceFailed=true;if(!continueRestoration)throw error;}
 };
 save();let published=false,relations,beforeSnapshot,nativeExecution;
 try{
  console.log(JSON.stringify({status:receipt.status,candidatePin,liveDirectory,receiptFile}));
  const result=await withSharedRehearsalTurn({task:'Install2 exact pushed WP-PP full rehearsal and managed TEST release',progressPath,
   async work(turn){
    receipt.turn=turn.identity;save();
    // An explicit owner pause can close only this owned turn. The monitor then
    // cancels only our rollback child and holds later publication. An already
    // dispatched protected job is preserved for exact-run reconciliation.
    let controlInput='';
    const pauseInput=chunk=>{controlInput+=chunk.toString();const lines=controlInput.split(/\r?\n/);controlInput=lines.pop();
     if(lines.includes('PAUSE')){receipt.ownerPauseRequestedAt=new Date().toISOString();
      try{save({continueRestoration:true});}finally{void turn.turnConnection.close().catch(()=>{receipt.pauseCloseFailed=true;});}}};
    process.stdin.on('data',pauseInput);
    try{
    const probeEnv=agencyChildEnvironment('healthroster_install2_probe');
    const probe=async sql=>{
     const r=await turn.runOwnedChild({file:psqlPath,args:['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],input:'begin isolation level repeatable read read only;\n'+sql+'\nrollback;\n',
      options:{env:{...probeEnv,PGOPTIONS:probeEnv.PGOPTIONS+' -c statement_timeout=0'},stdio:['pipe','pipe','pipe']},cancelOnLoss:true});
     requireThat(r.code===0&&!/(?:ERROR|FATAL):/.test(r.stderr),'NATIVE_READ_ONLY_PROBE_FAILED');
     return parseOneJsonResult(r.stdout);
    };
    const freshGate=async()=>{
     assertNoProtectedRelease();const bank=completedBankRecord();
     requireThat(git(['rev-parse','HEAD'])===candidatePin&&git(['status','--porcelain'])==='','INSTALL2_REVIEWED_SOURCE_CHANGED');
     const binding=assertRehearsalSourceBinding(sourceBinding,{review,expectedContractHash:contract});
     requireThat(remote('test')===(published?candidatePin:predecessor),'INSTALL2_SHARED_TEST_MOVED');
     const sourceRelationship=collectInstall2SourceRelationship({repositoryRoot:root,installedSourceCommit:config.installedSourceCommit,completedBankCommit:bank.sourcePin,expectedCandidateCommit:candidatePin,expectedRemoteTestCommit:predecessor,featureBranch:'codex/healthroster-install2-14102026'});
     const native=await probe(install2PredecessorProbeSql(marker));
     requireThat(Number.isSafeInteger(native.serverVersionNum)&&native.serverVersionNum>=170000&&native.serverVersionNum<180000,'INSTALL2_PG17_SERVER_REQUIRED');
     const identity=native.identity;
     const predecessorObservation={...identity,contract:native.contract,installed:native.installed,connectedFresh:true,checkedAt:new Date().toISOString()};
     const observed={...identity,checkedAt:new Date().toISOString(),environment:'TEST',candidatePin,pushedCandidatePin:remote('codex/healthroster-install2-14102026'),
      cleanGit:true,sourceStaged:true,reviewedSqlRuntimeMatch:true,sourceBindingSha256:binding.sha256,expectedContractHash:contract,remoteTestPin:predecessor,bank,
      protectedRunsChecked:true,protectedActiveCount:0,phoneOrAcceptanceActive:false,route:'APPROVED_MACHINE_LOCAL_FALLBACK_PROCESS_ENV',
      borrowedLabCapability:false,secretsInArgvOrFiles:false,ownerNativeCallbackAuthorised:true};
     return assertFreshInstall2NativeGate({...observed,predecessorObservation,reviewedInstalledSource,sourceRelationship,reviewedInstalledToRemotePaths:config.reviewedInstalledToRemotePaths},{candidatePin,currentRemoteTestPin:predecessor,expectedContractHash:contract});
    };
    const snapshot=async()=>{
     const census=await probe("select coalesce(jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind::text) order by n.nspname,c.relname),'[]'::jsonb) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p');");
     relations??=census;
     requireThat(JSON.stringify(census)===JSON.stringify(relations),'NATIVE_RESTORATION_RELATION_CENSUS_CHANGED');
     const result=await probe(restorationSnapshotSql({identity:turn.identity,marker,relations,exportContractSql:canonical(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8')),observeLiveBusiness:true}));
     return {...result,marker};
    };
    return await runFullRehearsalWithinOwnedTurn({installNumber:2,turn,candidatePin,currentRemoteTestPin:predecessor,expectedContractHash:contract,marker,review,callbacks:{
     freshGate,
     async recordReceipt(value){receipt.checkpoints??=[];receipt.checkpoints.push({...value,...value.execution?{execution:publicExecution(value.execution)}:{}});receipt.status=value.status;save({continueRestoration:true});},
     async before(){const before=await snapshot();before.installed=await probe("select jsonb_build_object('migrations',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256) order by path),'[]'::jsonb) from private.cloudtms_migration_ledger),'repeatables',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger));");
      before.contractDigest={...await probe(expectedContractDigestSql({expectedContractHash:contract})),connectedFresh:true,checkedAt:new Date().toISOString()};before.observeLiveBusiness=true;beforeSnapshot=before;receipt.before=publicSnapshot(before);save();return before;},
     async executeSameSession(input){receipt.status='FULL_REHEARSAL_RUNNING';receipt.sqlHash=input.sqlHash;receipt.mappedStatements=input.lineMap.length;save();
      const units=[...input.sql.matchAll(/^\\echo HR_FILE_BEGIN (\d+)$/gm)].map(match=>{
       const line=input.sql.slice(0,match.index).split('\n').length;
       return {index:Number(match[1]),file:input.lineMap.find(row=>row.startLine>line)?.file??'HARNESS_CONTROL'};});
      const observer=createRehearsalStreamObserver({units,lineMap:input.lineMap,allowedCodes:new Set(input.diagnosticCodes),onRecord(record){
       const value={...record,at:new Date().toISOString()};
       if(record.kind==='UNIT_PROGRESS')receipt.progress=value;
       else if(record.kind==='PARTIAL_TRANSACTION_IDENTITY')receipt.partialTransactionIdentity=value;
       else {receipt.partialDiagnostics??=[];receipt.partialDiagnostics.push(value);}
       try{journal.checkpoint(value,{progress:true});}catch(error){receipt.receiptPersistenceFailed=true;throw error;}
      }});
      const local=Object.fromEntries(fs.readFileSync(path.join(workspace,'cloudtms-backend/.codex-tmp/miget-poc.env'),'utf8')
       .split(/\r?\n/).filter(line=>line.includes('=')&&!line.trim().startsWith('#')).map(line=>{const at=line.indexOf('=');return[line.slice(0,at).trim(),line.slice(at+1).trim()];}));
      requireThat(!!local.MIGET_API_TOKEN,'OWN_APPROVED_RESOURCE_OBSERVATION_TOKEN_REQUIRED');
      const readSample=createMigetRehearsalResourceReader({headers:{authorization:'Bearer '+local.MIGET_API_TOKEN,accept:'application/json',
       ...local.MIGET_WORKSPACE_ID?{'X-Workspace-Id':local.MIGET_WORKSPACE_ID}:{}}});
      const resourceGuard=createRehearsalResourceGuard({readSample,onSample(sample){
       receipt.resourceProgress=sample;receipt.resourceMaxima={memoryUsed:Math.max(receipt.resourceMaxima?.memoryUsed??0,sample.memoryUsed),
        volumeUsed:Math.max(receipt.resourceMaxima?.volumeUsed??0,sample.volumeUsed)};
       journal.checkpoint({kind:'OWN_REHEARSAL_RESOURCE_OBSERVATION',...sample},{progress:true});
      }});
      receipt.resourceLimits=await resourceGuard.prepare();save();
      const observedTurn={...turn,runOwnedChild:options=>turn.runOwnedChild({...options,resourceGuard,options:{...options.options,onStdout:observer.onStdout,onStderr:observer.onStderr}})};
      let execution;try{execution=await executePsqlWithinOwnedTurn({...input,turn:observedTurn,psqlPath,env:agencyChildEnvironment('healthroster_install2_full')});}
      finally{receipt.partialStreamSummary=observer.summary();receipt.resourceGuard=await resourceGuard.stop();save({continueRestoration:true});}
      nativeExecution=execution;receipt.execution=publicExecution(execution);receipt.errors=execution.errors;receipt.status='FULL_REHEARSAL_SQL_COMPLETED';save();return execution;},
     async freshAfter(){const after={...await snapshot(),connectedFresh:true};receipt.after=publicSnapshot(after);
      const prior=new Map((receipt.before?.businessRelations??[]).map(row=>[row.relation,row]));
      receipt.businessChanges=(after.businessRelations??[]).filter(row=>row.count!==prior.get(row.relation)?.count||row.hash!==prior.get(row.relation)?.hash)
       .map(row=>({relation:row.relation,beforeCount:prior.get(row.relation)?.count,afterCount:row.count,beforeHash:prior.get(row.relation)?.hash,afterHash:row.hash}));
      requireThat(nativeExecution?.transaction&&Array.isArray(nativeExecution.fixtureObservations),'REHEARSAL_FIXTURE_EXECUTION_CAPTURE_REQUIRED');
      const fixtureProof=await probe(freshFixtureProofSql(nativeExecution.transaction,nativeExecution.fixtureObservations,beforeSnapshot));
      const expectedWriters=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8')).routines
       .filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity))
       .sort((a,b)=>(a.schema+'|'+a.identity).localeCompare(b.schema+'|'+b.identity));
      requireThat(expectedWriters.length===4,'REHEARSAL_BACKGROUND_EXPECTED_WRITERS_INCOMPLETE');
      const sortWriters=rows=>[...rows].sort((a,b)=>(a.schema+'|'+a.identity).localeCompare(b.schema+'|'+b.identity));
      assert.deepEqual(sortWriters(beforeSnapshot.backgroundWriterRoutines),expectedWriters,'REHEARSAL_BACKGROUND_BEFORE_WRITER_DRIFT');
      assert.deepEqual(sortWriters(after.backgroundWriterRoutines),expectedWriters,'REHEARSAL_BACKGROUND_AFTER_WRITER_DRIFT');
      const writerProof={matched:true,routines:expectedWriters.map(r=>({schema:r.schema,identity:r.identity,definitionSha256:r.definition_sha256})),
       authority:'exact sealed contract and unchanged native before/after catalogue; actual 29082026 claim and existing dequeue/fail owners'};
      after.liveRestoration=qualifyLiveRestoration(beforeSnapshot,after,{transaction:nativeExecution.transaction,observations:nativeExecution.fixtureObservations,
       expectedUnits:nativeExecution.fixtureObservationUnits,fixtureProof,writerProof});
      receipt.fixtureProof=fixtureProof;receipt.liveRestoration=after.liveRestoration;
      save({continueRestoration:true});return after;},
     async publishManaged({receipt:clean}){
      requireThat(!receipt.receiptPersistenceFailed,'INSTALL2_RECEIPT_PERSISTENCE_FAILED_HOLD');
      receipt.cleanRehearsal=clean;receipt.status='FULL_CLEAN_AWAITING_FRESH_CONNECTION_PROOF';save();
      const proofFile=path.join(root,'.codex-tmp/healthroster-install2-connections-proof.json');
      console.log(JSON.stringify({status:receipt.status,candidatePin,receiptFile,proofFile}));
      for(;;){await turn.assertHeld('await-fresh-connections-proof');
       if(fs.existsSync(proofFile)){let proof;try{proof=JSON.parse(fs.readFileSync(proofFile,'utf8'));validateConnectionProof(proof,candidatePin,targets);}catch{proof=null;}
        if(proof){requireThat(targets.every(([,worker,branch])=>remote(branch)===proof.workers.find(row=>row.worker===worker).branchCommit),'INSTALL2_DEPLOYMENT_BRANCH_MOVED');break;}}
       await abortableDelay(5000,turn.signal);
      }
      await freshGate();requireThat(remote('test')===predecessor,'INSTALL2_PROMOTION_PREDECESSOR_CHANGED');
      requireThat(git(['merge-base',predecessor,candidatePin])===predecessor,'INSTALL2_NONFORCED_ANCESTRY_REQUIRED');
      await turn.assertHeld('before-reviewed-test-promotion');
      git(['push','origin',candidatePin+':refs/heads/test']);published=true;
      requireThat(remote('test')===candidatePin,'INSTALL2_PROMOTION_NOT_VERIFIED');
      receipt.status='MANAGED_RELEASE_RUNNING';save();
      const publication=await turn.runManagedRelease({npmCli:'C:/Program Files/nodejs/node_modules/npm/bin/npm-cli.js',connectionsProof:proofFile,officePath:config.officePath,cwd:root,env:{...process.env,GH_BIN:gh}});
      requireThat(publication.code===0,'INSTALL2_MANAGED_RELEASE_FAILED_RECONCILE_OWN_RECEIPT');
      const deployment=JSON.parse(fs.readFileSync(path.join(root,'.codex-tmp/test-deployment-receipt.json'),'utf8'));
      requireThat(deployment.commit===candidatePin&&deployment.status==='DEPLOYED_ACCEPTANCE_PENDING','INSTALL2_MANAGED_RECEIPT_NOT_COMPLETE');
      receipt.publication=deployment;receipt.status='DEPLOYED_ACCEPTANCE_PENDING';save();return deployment;
     }
    }});
    }finally{process.stdin.removeListener('data',pauseInput);process.stdin.pause();}
   }},{
    readLastBusy(){
     const records=fs.readdirSync(turnJournal).filter(name=>/^[a-f0-9-]{36}\.json$/.test(name))
      .map(name=>JSON.parse(fs.readFileSync(path.join(turnJournal,name),'utf8')));
     if(fs.existsSync(progressPath))for(const line of fs.readFileSync(progressPath,'utf8').split(/\r?\n/))
      if(line.startsWith('SEND_7 turn: '))records.push(JSON.parse(line.slice('SEND_7 turn: '.length)));
     return latestBusyTimestamp(records);
    },
    log(record){
     // This immutable authoritative cooldown record survives a failed later
     // OneDrive archive and is consulted before the next TEST connection.
     fs.writeFileSync(path.join(turnJournal,crypto.randomUUID()+'.json'),JSON.stringify(record)+'\n',{flag:'wx'});
     fs.appendFileSync(path.join(liveDirectory,'SHARED_TURN.md'),'\nSEND_7 turn: '+JSON.stringify(record)+'\n');
    }
   });
  receipt.endedAt=new Date().toISOString();receipt.result=result;receipt.status=result?.publication?'DEPLOYED_ACCEPTANCE_PENDING':result?.status??'STOPPED';save();
  console.log(JSON.stringify({status:receipt.status,candidatePin,receiptFile}));return receipt;
 }catch(error){receipt.status='HELD_NOT_COMPLETE';receipt.failure=/^[A-Z][A-Z0-9_]+$/.test(error.code??error.message)?error.code??error.message:'NATIVE_FAILURE_WITHHELD';receipt.endedAt=new Date().toISOString();try{save();}catch{console.error('RECEIPT_CHECKPOINT_WRITE_FAILED');}console.error(receipt.failure);process.exitCode=1;return receipt;}
 finally{
  // The live turn log is already closed. Archiving is outside the database
  // critical section and cannot orphan a running child or held turn.
  try{if(fs.existsSync(path.join(liveDirectory,'SHARED_TURN.md')))fs.appendFileSync(progressPath,'\n'+fs.readFileSync(path.join(liveDirectory,'SHARED_TURN.md'),'utf8'));}
  catch{console.error('TURN_LOG_ARCHIVE_PENDING');}
  try{fs.mkdirSync(out,{recursive:true});fs.writeFileSync(path.join(out,'INSTALL2_FULL_REHEARSAL_'+stamp+'.json'),JSON.stringify({...receipt,liveDirectory,receiptFile},null,2)+'\n',{flag:'wx'});}
  catch{console.error('RECEIPT_ARCHIVE_PENDING');}
 }
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))await runNativeInstall2();
