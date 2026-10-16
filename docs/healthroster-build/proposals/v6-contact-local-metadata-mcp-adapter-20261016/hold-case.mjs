// ROOT-ONLY fixed metadata + own A coordinator. B is ONLY server-owned Kier MCP.
// Full canonical maps exist only in this live process, never disk or console.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {source,buildInput,holderProofSql,requireThat,literal,ROUTE} from './input-builder.mjs';
import {readStageOne,prepareQualification,validatePair} from './result-validator.mjs';
import {createFixedMetadataReader} from '../v6-contact-large-frame-feasibility-20261016/fixed-metadata-reader.mjs';
import {BUDGET,FiniteControls,assertElapsed,assertNoMaps} from './control.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const closed=error=>/^[A-Z0-9_]+$/.test(error?.message??'')?error.message:'CONTACT_LOCAL_METADATA_CLOSED_FAILURE';
const one=lines=>{requireThat(lines.length===1,'HOLDER_ONE_FRAME_REQUIRED');try{return JSON.parse(lines[0]);}catch{throw Error('HOLDER_CONTROL_JSON_REFUSED');}};
function serialized(connection){const query=connection.query.bind(connection);let queue=Promise.resolve();connection.query=sql=>{const next=queue.then(()=>query(sql));queue=next.catch(()=>{});return next;};return connection;}
function saveSmall(out,name,value){assertNoMaps(value);const text=JSON.stringify(value,null,2)+'\n';requireThat(Buffer.byteLength(text)<=BUDGET.smallFileBytes,'SMALL_PROTOCOL_FILE_BOUND');fs.writeFileSync(path.join(out,name),text);}
function readSmall(out,name){
 const file=path.join(out,name),stat=fs.lstatSync(file);requireThat(stat.isFile()&&!stat.isSymbolicLink()&&stat.size>0&&stat.size<=BUDGET.smallFileBytes,'SMALL_NATIVE_RESULT_REQUIRED');
 const text=fs.readFileSync(file,'utf8');requireThat(Buffer.byteLength(text)<=BUDGET.smallFileBytes,'SMALL_NATIVE_RESULT_REQUIRED');
 try{return JSON.parse(text);}catch{throw Error('SMALL_NATIVE_JSON_REFUSED');}
}
export async function cleanupWitness(connection,plan){
 const proof=one(await connection.query(`rollback; select jsonb_build_object('physicalDatabase',current_database(),'backendPid',pg_backend_pid(),
  'witnessLocksGone',not exists(select 1 from pg_locks where pid=pg_backend_pid() and locktype='relation' and relation in (${plan.relationNames.map(n=>literal(n)+'::regclass').join(',')})));`));
 requireThat(proof.physicalDatabase==='cloudtms_test_clone'&&proof.witnessLocksGone===true,'OWNED_HOLDER_LOCK_RELEASE_UNPROVED');return proof;
}
export async function holdCase({root,out,caseId,authorisedRootHolder=false}){
 requireThat(authorisedRootHolder===true,'ROOT_HOLDER_AUTHORIZATION_REQUIRED');
 const {plan}=await source(root),row=plan.cases.find(r=>r.id===caseId);requireThat(row,'EXACT_CASE_REQUIRED');
 // Verify this exact local adapter/fixed-reader source before any connection.
 const manifest=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'),'utf8'));
 const {sha}=await import('./input-builder.mjs');
 for(const f of manifest.files)requireThat(sha(fs.readFileSync(path.join(here,f.file)))===f.sha256,'ADAPTER_SOURCE_CHANGED');
 for(const f of manifest.pinnedPredecessorFiles)requireThat(sha(fs.readFileSync(path.join(here,f.file)))===f.sha256,'FIXED_READER_SOURCE_CHANGED');
 requireThat(path.dirname(path.resolve(out))===path.join(path.resolve(root),'.codex-tmp')&&/^contact-six-local-mcp-20261016-[a-z0-9-]+$/.test(path.basename(out))&&!fs.existsSync(out),'FRESH_ROOT_OUTPUT_DIRECTORY_REQUIRED');
 fs.mkdirSync(out,{recursive:true});const native=await import(pathToFileURL(path.join(root,'scripts/healthroster-rehearsal-turn.mjs')).href);
 let sequenceStart,cleanupProof;
 try{
  const result=await native.withSharedRehearsalTurn({task:'HEALTH_CONTACT_LOCAL_METADATA_MCP_'+caseId.toUpperCase(),progressPath:path.join(root,'docs/healthroster-build/PROGRESS.md'),
   async work(turn){
    sequenceStart=performance.now();const workDeadline=new AbortController();
    const timer=setTimeout(()=>workDeadline.abort(Error('WHOLE_SEQUENCE_WORK_BUDGET_HELD')),BUDGET.workMilliseconds);
    const controls=new FiniteControls(process.stdin,AbortSignal.any([turn.signal,workDeadline.signal]));
    const owned={...turn,signal:AbortSignal.any([turn.signal,workDeadline.signal,controls.signal])};
    let beforePacket,afterPacket,nativeResult,summary;
    const within=()=>{assertElapsed(sequenceStart,performance.now(),BUDGET.workMilliseconds);if(owned.signal.aborted)throw owned.signal.reason;};
    try{
     within();const reader=await createFixedMetadataReader({root,turn:owned,caseId});
     beforePacket=await reader.before();within();
     // C-before closed before A takes any witness table lock. In particular A
     // must never retain ACCESS SHARE on all six through a snapshot read.
     requireThat(beforePacket.childClosed===true,'C_BEFORE_CLOSE_REQUIRED');
     const shape=one(await turn.turnConnection.query(`select jsonb_build_object('valid',(select count(*)=6 from pg_class c join pg_namespace n on n.oid=c.relnamespace
      where n.nspname||'.'||c.relname in (${plan.relationNames.map(literal).join(',')}) and c.relkind='r' and c.relpersistence='p' and not c.relispartition
      and c.relowner=(select oid from pg_roles where rolname=current_user) and not exists(select 1 from pg_inherits i where i.inhrelid=c.oid or i.inhparent=c.oid)),
      'noWitnessHeld',not exists(select 1 from pg_locks where pid=pg_backend_pid() and locktype='relation' and relation in (${plan.relationNames.map(n=>literal(n)+'::regclass').join(',')})));`));
     requireThat(shape.valid===true&&shape.noWitnessHeld===true,'EXACT_NATIVE_UNHELD_WITNESS_SHAPE_REQUIRED');within();
     await turn.turnConnection.query(`begin ${row.holderReadOnly?'read only':'read write'}; set local lock_timeout='2s'; ${row.holderLockSql}`);
     const proof=one(await turn.turnConnection.query(holderProofSql(row,plan.relationNames)));
     requireThat(proof.backendPid===turn.identity.backendPid&&proof.databaseOid===turn.identity.databaseOid&&proof.physicalDatabase==='cloudtms_test_clone'
      &&proof.readOnly===row.holderReadOnly&&proof.targetGranted===true&&proof.onlyTargetWitness===true,'EXACT_OWNED_HOLDER_REQUIRED');within();
     const built=await buildInput({root,caseId,identity:turn.identity});
     requireThat(built.receipt.beforeCanonicalSqlSha256===reader.sourceDescriptor.canonicalSqlSha256,'LOCAL_CANONICAL_SOURCE_REVERSE_REQUIRED');
     saveSmall(out,'TURN_IDENTITY.json',turn.identity);saveSmall(out,'HOLDER_PROOF.json',proof);
     saveSmall(out,'INPUT.json',built.input);saveSmall(out,'INPUT_RECEIPT.json',built.receipt);
     const first=controls.wait('PREPARE');
     console.log(JSON.stringify({status:'READY_ROOT_KIER_MCP_B_REQUIRED',caseId,out,route:ROUTE,remainingWorkMilliseconds:Math.floor(BUDGET.workMilliseconds-(performance.now()-sequenceStart)),
      expectedFailedStatement:built.receipt.indices.expectedFailedStatement,expectedSqlstate:'55P03',canonicalMetadata:'PROCESS_MEMORY_ONLY'}));
     await first;within();nativeResult=readSmall(out,'NATIVE_RESULT.json');const stage=readStageOne(nativeResult,built.receipt);
     afterPacket=await reader.after({nativeBackendPid:stage.transaction.backendPid});within();
     const qualificationInput=await prepareQualification({root,result:nativeResult,receipt:built.receipt,beforePacket,afterPacket});
     saveSmall(out,'QUALIFICATION_INPUT.json',qualificationInput);
     const second=controls.wait('RELEASE');
     console.log(JSON.stringify({status:'PREPARED_ROOT_SECOND_KIER_MCP_FIXTURE_PROOF_REQUIRED',caseId,out,route:ROUTE,remainingWorkMilliseconds:Math.floor(BUDGET.workMilliseconds-(performance.now()-sequenceStart))}));
     await second;within();
     summary=await validatePair({root,result:nativeResult,receipt:built.receipt,beforePacket,afterPacket,qualificationInput,qualificationResult:readSmall(out,'QUALIFICATION_RESULT.json')});within();
     return summary;
    }finally{
     clearTimeout(timer);controls.dispose();
     // No raw maps enter a file even on error. Canonical weak qualification was
     // already computed on the complete original objects in this same process.
     beforePacket=null;afterPacket=null;nativeResult=null;
     cleanupProof=await cleanupWitness(turn.turnConnection,plan);
     requireThat(cleanupProof.backendPid===turn.identity.backendPid,'OWNED_CLEANUP_PID_REFUSED');
     saveSmall(out,'HOLDER_ROLLBACK.json',{caseId,...cleanupProof,status:summary?'QUALIFIED_PENDING_SHARED_TURN_CLOSE':'HOLD_ABORTED'});
    }
   }},{openTurn:()=>serialized(new native.OwnedTurnConnection(native.agencyChildEnvironment('healthroster_contact_local_metadata_holder')))});
  if(sequenceStart===undefined){
   requireThat(['BUSY_NO_TRIAL','COOLDOWN_NO_CONNECTION'].includes(result?.status),'SHARED_TURN_CALLBACK_NOT_RUN');
   const held={...result,caseId,native:'NOT_RUN',completeMapsPersisted:false};saveSmall(out,'HOLD.json',held);console.log(JSON.stringify(held));return held;
  }
  const elapsed=assertElapsed(sequenceStart);
  requireThat(cleanupProof?.witnessLocksGone===true,'OWNED_CLEANUP_REQUIRED');
  const final={...result,status:'PASS_EXPECTED_EXACT_LOCK_REFUSAL_AND_FULL_CANONICAL_LIVE_RESTORATION',
   elapsedMilliseconds:Math.ceil(elapsed),wholeSequenceBudgetMilliseconds:BUDGET.wholeMilliseconds,workBudgetMilliseconds:BUDGET.workMilliseconds,
   witnessLocksGone:true,canonicalSharedTurnUnlockedAndClosed:true,completeMapsPersisted:false};
  saveSmall(out,'SUMMARY.json',final);console.log(JSON.stringify({status:final.status,caseId,elapsedMilliseconds:final.elapsedMilliseconds}));return final;
 }catch(error){
  saveSmall(out,'HOLD.json',{status:'HOLD',caseId,code:closed(error),witnessCleanupProved:cleanupProof?.witnessLocksGone===true,
   sharedTurnCompletion:'SEE_CANONICAL_OWNER_PROGRESS_LOG_NO_SYNTHETIC_CLOSE_PROOF',nativeResultRequiredBeforeAnyNextTrial:true,
   elapsedMilliseconds:sequenceStart===undefined?null:Math.ceil(performance.now()-sequenceStart),completeMapsPersisted:false});throw Error(closed(error));
 }
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const [root,out,caseId,flag]=process.argv.slice(2);requireThat(flag==='--authorised-root-holder','ROOT_HOLDER_AUTHORIZATION_REQUIRED');await holdCase({root,out,caseId,authorisedRootHolder:true});}
 catch(error){console.error(JSON.stringify({status:'HOLD',code:closed(error)}));process.exitCode=1;}
}
