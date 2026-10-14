// Owner-authorised Install1 only. Importing this module opens no connection.
// SQL credentials remain in the approved child environment, never in argv/artifacts.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {withSharedRehearsalTurn,agencyChildEnvironment,assertNoProtectedRelease} from './healthroster-rehearsal-turn.mjs';
import {abortableDelay} from './healthroster-turn-control.mjs';
import {runFullRehearsalWithinOwnedTurn,executePsqlWithinOwnedTurn,restorationSnapshotSql,identityProbeSql,rehearsalSourceBinding,assertRehearsalSourceBinding,expectedContractDigestSql,assertFreshNativeGate} from './healthroster-install1-full-rehearsal.mjs';
import {validateConnectionProof} from './automatic-test-release-policy.mjs';
import {canonical,digest} from './cloudtms-managed-release-sql.mjs';
import {createReceiptJournal,latestBusyTimestamp} from './healthroster-receipt-journal.mjs';
import {freshFixtureProofSql,qualifyLiveRestoration} from './healthroster-live-test-restoration.mjs';
import assert from 'node:assert/strict';
import {createRehearsalStreamObserver} from './healthroster-rehearsal-stream-observer.mjs';
import {createRehearsalResourceGuard,createMigetRehearsalResourceReader} from './healthroster-rehearsal-resource-guard.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const workspace='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub';
const predecessor='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';
const contract='7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6';
const psqlPath='C:/Program Files/PostgreSQL/18/bin/psql.exe';
const gh='C:/Program Files/GitHub CLI/gh.exe';
const out=path.join(workspace,'cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_BUILD_13102026');
const progressPath=path.join(workspace,'.codex-worktrees/healthroster-v2-backend-13102026/docs/healthroster-build/PROGRESS.md');
const targets=[['backend','test-cloudtms-backend','deploy/cloudflare/test-cloudtms-backend'],['private','test-cloudtms-candidate-private-api','deploy/cloudflare/test-candidate-private-api'],['synthetic','test-cloudtms-candidate-synthetic-private-api','deploy/cloudflare/test-candidate-synthetic-private-api'],['broker','test-cloudtms-candidate-broker','deploy/cloudflare/test-candidate-broker']];
const requireThat=(condition,code)=>{if(!condition)throw Error(code);};
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,encoding:'utf8',windowsHide:true,maxBuffer:16000000}).trim();
const remote=branch=>git(['ls-remote','origin','refs/heads/'+branch]).split(/\s/)[0];
const publicSnapshot=value=>{const {backgroundRows,backgroundSnapshotXip,...safe}=value;return safe;};
const publicExecution=value=>{if(!value)return value;const {fixtureObservations,...safe}=value;return {...safe,fixtureObservationCount:fixtureObservations?.length??0};};

export function parseOneJsonResult(stdout){
 const lines=stdout.split(/\r?\n/).filter(x=>x.trim().startsWith('{')||x.trim().startsWith('['));
 requireThat(lines.length===1,'NATIVE_PROBE_JSON_ENVELOPE_REFUSED');
 try{return JSON.parse(lines[0]);}catch{throw Error('NATIVE_PROBE_JSON_INVALID');}
}

function completedBankRecord(){
 const file=path.join(workspace,'.codex-worktrees/bpay-next-build-backend-20261009/docs/bpay-next/TEST_INSTALLS.md');
 const text=fs.readFileSync(file,'utf8');
 requireThat(text.includes('First disabled-first install — COMPLETE')&&text.includes(predecessor)
  &&text.includes('pg_advisory_unlock returned true')&&text.includes('2026-10-13T23:45:36.217Z'),'BANK_COMPLETED_RELEASE_RECORD_REQUIRED');
 const run=JSON.parse(execFileSync(gh,['api','repos/kierarthur/cloudtms-backend/actions/runs/38476430690'],{encoding:'utf8',windowsHide:true}));
 requireThat(run.status==='completed'&&run.conclusion==='success'&&run.head_sha===predecessor,'BANK_PROTECTED_COMPLETION_CHANGED');
 return {status:'COMPLETE',protectedConclusion:'success',sourcePin:predecessor,sharedUnlockConfirmed:true,recordHash:digest(text)};
}

export async function runNativeInstall1(){
 requireThat(process.argv[2]==='--run-owner-authorised-install1','EXPLICIT_OWNER_INSTALL1_ENTRY_REQUIRED');
 requireThat(git(['branch','--show-current'])==='codex/healthroster-install1-14102026','INSTALL1_OWN_BRANCH_REQUIRED');
 requireThat(!git(['status','--porcelain']),'INSTALL1_CLEAN_PUSHED_SOURCE_REQUIRED');
 const candidatePin=git(['rev-parse','HEAD']);
 requireThat(remote('codex/healthroster-install1-14102026')===candidatePin&&remote('test')===predecessor,'INSTALL1_SOURCE_REMOTE_CHANGED');
 const archiveProof=JSON.parse(fs.readFileSync(path.join(root,'.codex-tmp/healthroster-install1-archive-preflight.json'),'utf8'));
 requireThat(archiveProof.status==='SOURCE_PREFLIGHT_PASS'&&archiveProof.commit===candidatePin&&archiveProof.linearAncestryPassed===true
  &&archiveProof.source==='EXACT_COMMITTED_GIT_ARCHIVE'&&archiveProof.sqlCarriageReturnFiles?.length===0
  &&archiveProof.steps?.length===9&&archiveProof.steps.every(row=>row.exitCode===0)&&archiveProof.cleanup?.removed===true
  &&archiveProof.dependencyProvisioning?.exitCode===0&&archiveProof.dependencyProvisioning.installerHooks===false
  &&archiveProof.dependencyProvisioning.lockUnchanged===true
  &&archiveProof.dependencyProvisioning.packageLockSha256===digest(canonical(fs.readFileSync(path.join(root,'package-lock.json'),'utf8'))),
  'INSTALL1_COMMITTED_ARCHIVE_PREFLIGHT_REQUIRED');
 requireThat(digest(JSON.stringify(JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8'))))===contract,'INSTALL1_SEALED_CONTRACT_CHANGED');
 process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
 const review=JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/install1-rehearsal/VERIFIER_SAFETY_REVIEW.json'),'utf8'));
 const sourceBinding=rehearsalSourceBinding({review});
 assertRehearsalSourceBinding(sourceBinding,{review,expectedContractHash:contract});
 const marker='hr_install1_'+crypto.randomBytes(16).toString('hex');
 const stamp=new Date().toISOString().replace(/[-:]/g,'').replace(/\.\d+Z$/,'Z');
 const liveDirectory=fs.mkdtempSync('C:/tmp/healthroster-install1-native-'+stamp+'-');
 const journal=createReceiptJournal(liveDirectory);
 const turnJournal='C:/tmp/healthroster-install1-turn-journal-20261014';
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
  const result=await withSharedRehearsalTurn({task:'Install1 exact pushed WP-PP full rehearsal and managed TEST release',progressPath,
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
    const probeEnv=agencyChildEnvironment('healthroster_install1_probe');
    const probe=async sql=>{
     const r=await turn.runOwnedChild({file:psqlPath,args:['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],input:'begin isolation level repeatable read read only;\n'+sql+'\nrollback;\n',
      options:{env:{...probeEnv,PGOPTIONS:probeEnv.PGOPTIONS+' -c statement_timeout=0'},stdio:['pipe','pipe','pipe']},cancelOnLoss:true});
     requireThat(r.code===0&&!/(?:ERROR|FATAL):/.test(r.stderr),'NATIVE_READ_ONLY_PROBE_FAILED');
     return parseOneJsonResult(r.stdout);
    };
    const freshGate=async()=>{
     assertNoProtectedRelease();const bank=completedBankRecord();
     requireThat(git(['rev-parse','HEAD'])===candidatePin&&git(['status','--porcelain'])==='','INSTALL1_REVIEWED_SOURCE_CHANGED');
     const binding=assertRehearsalSourceBinding(sourceBinding,{review,expectedContractHash:contract});
     requireThat(remote('test')===(published?candidatePin:predecessor),'INSTALL1_SHARED_TEST_MOVED');
     const identity=await probe(identityProbeSql(marker));
     const observed={...identity,checkedAt:new Date().toISOString(),environment:'TEST',candidatePin,pushedCandidatePin:remote('codex/healthroster-install1-14102026'),
      cleanGit:true,sourceStaged:true,reviewedSqlRuntimeMatch:true,sourceBindingSha256:binding.sha256,expectedContractHash:contract,remoteTestPin:predecessor,bank,
      protectedRunsChecked:true,protectedActiveCount:0,phoneOrAcceptanceActive:false,route:'APPROVED_MACHINE_LOCAL_FALLBACK_PROCESS_ENV',
      borrowedLabCapability:false,secretsInArgvOrFiles:false,ownerNativeCallbackAuthorised:true};
     return assertFreshNativeGate(observed,{candidatePin,currentRemoteTestPin:predecessor,expectedContractHash:contract});
    };
    const snapshot=async()=>{
     const census=await probe("select coalesce(jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind::text) order by n.nspname,c.relname),'[]'::jsonb) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p');");
     relations??=census;
     requireThat(JSON.stringify(census)===JSON.stringify(relations),'NATIVE_RESTORATION_RELATION_CENSUS_CHANGED');
     const result=await probe(restorationSnapshotSql({identity:turn.identity,marker,relations,exportContractSql:canonical(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8')),observeLiveBusiness:true}));
     return {...result,marker};
    };
    return await runFullRehearsalWithinOwnedTurn({turn,candidatePin,currentRemoteTestPin:predecessor,expectedContractHash:contract,marker,review,callbacks:{
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
      let execution;try{execution=await executePsqlWithinOwnedTurn({...input,turn:observedTurn,psqlPath,env:agencyChildEnvironment('healthroster_install1_full')});}
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
      requireThat(!receipt.receiptPersistenceFailed,'INSTALL1_RECEIPT_PERSISTENCE_FAILED_HOLD');
      receipt.cleanRehearsal=clean;receipt.status='FULL_CLEAN_AWAITING_FRESH_CONNECTION_PROOF';save();
      const proofFile=path.join(root,'.codex-tmp/healthroster-install1-connections-proof.json');
      console.log(JSON.stringify({status:receipt.status,candidatePin,receiptFile,proofFile}));
      for(;;){await turn.assertHeld('await-fresh-connections-proof');
       if(fs.existsSync(proofFile)){let proof;try{proof=JSON.parse(fs.readFileSync(proofFile,'utf8'));validateConnectionProof(proof,candidatePin,targets);}catch{proof=null;}
        if(proof){requireThat(targets.every(([,worker,branch])=>remote(branch)===proof.workers.find(row=>row.worker===worker).branchCommit),'INSTALL1_DEPLOYMENT_BRANCH_MOVED');break;}}
       await abortableDelay(5000,turn.signal);
      }
      await freshGate();requireThat(remote('test')===predecessor,'INSTALL1_PROMOTION_PREDECESSOR_CHANGED');
      requireThat(git(['merge-base',predecessor,candidatePin])===predecessor,'INSTALL1_NONFORCED_ANCESTRY_REQUIRED');
      await turn.assertHeld('before-reviewed-test-promotion');
      git(['push','origin',candidatePin+':refs/heads/test']);published=true;
      requireThat(remote('test')===candidatePin,'INSTALL1_PROMOTION_NOT_VERIFIED');
      receipt.status='MANAGED_RELEASE_RUNNING';save();
      const publication=await turn.runManagedRelease({npmCli:'C:/Program Files/nodejs/node_modules/npm/bin/npm-cli.js',connectionsProof:proofFile,cwd:root,env:{...process.env,GH_BIN:gh}});
      requireThat(publication.code===0,'INSTALL1_MANAGED_RELEASE_FAILED_RECONCILE_OWN_RECEIPT');
      const deployment=JSON.parse(fs.readFileSync(path.join(root,'.codex-tmp/test-deployment-receipt.json'),'utf8'));
      requireThat(deployment.commit===candidatePin&&deployment.status==='DEPLOYED_ACCEPTANCE_PENDING','INSTALL1_MANAGED_RECEIPT_NOT_COMPLETE');
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
  try{fs.writeFileSync(path.join(out,'INSTALL1_FULL_REHEARSAL_'+stamp+'.json'),JSON.stringify({...receipt,liveDirectory,receiptFile},null,2)+'\n',{flag:'wx'});}
  catch{console.error('RECEIPT_ARCHIVE_PENDING');}
 }
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))await runNativeInstall1();
