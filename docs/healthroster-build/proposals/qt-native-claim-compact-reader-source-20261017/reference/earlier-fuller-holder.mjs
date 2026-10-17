// Root-owned operator. This source proposal must not be invoked before independent review.
// A and fixed read-only C are local; B is only the exact server-owned Kier rollback input.
import fs from'node:fs';import path from'node:path';import {fileURLToPath,pathToFileURL}from'node:url';
import {source,buildInput,requireThat,ROUTE}from'./input-builder.mjs';
import {assertCommittedAdapter,assertManifestedFixtureEntry,CASES}from'./source.mjs';
import {readStage,validateEligibleClaimRestoration as validateRestoration,nativeClockFrames}from'./result-validator.mjs';
import {rootCallTiming,WORK_DEADLINE_CODE}from'./timing.mjs';import {createFixedEligibleClaimMetadataReader as createFixedBatchMetadataReader}from'./fixed-metadata-reader.mjs';
import {BUDGET,FiniteControls,assertElapsed,assertNoMaps}from'./control.mjs';
import {metadataDiagnosticFor}from'./metadata-diagnostic.mjs';
const closed=e=>/^[A-Z0-9_]+$/.test(e?.message??'')?e.message:'CONTACT_QT_CLOSED_FAILURE';
function serialized(connection){const query=connection.query.bind(connection);let queue=Promise.resolve();connection.query=s=>{const next=queue.then(()=>query(s));queue=next.catch(()=>{});return next;};return connection;}
export function saveSmall(out,name,value){assertNoMaps(value);const text=JSON.stringify(value,null,2)+'\n';requireThat(Buffer.byteLength(text)<=BUDGET.smallFileBytes,'CONTACT_QT_SMALL_FILE_BOUND');fs.writeFileSync(path.join(out,name),text,{flag:'wx'});}
export function readSmall(out,name){const file=path.join(out,name),stat=fs.lstatSync(file);requireThat(stat.isFile()&&!stat.isSymbolicLink()&&stat.size>0&&stat.size<=BUDGET.smallFileBytes,'CONTACT_QT_SMALL_FILE_REQUIRED');const b=fs.readFileSync(file);requireThat(b.length<=BUDGET.smallFileBytes,'CONTACT_QT_SMALL_FILE_REQUIRED');try{return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(b));}catch{throw Error('CONTACT_QT_SMALL_JSON_REFUSED');}}
export async function holdCase({root,out,caseId,authorisedRootHolder=false}){
 requireThat(authorisedRootHolder===true&&CASES.includes(caseId),'CONTACT_QT_ROOT_AUTHORIZATION_AND_EXACT_CASE_REQUIRED');
 assertCommittedAdapter();assertManifestedFixtureEntry(root);await source(root,caseId);
 const parent=path.join(path.resolve(root),'.codex-tmp');requireThat(path.dirname(path.resolve(out))===parent&&/^contact-qt-positive-native-20261017-[a-z0-9-]+$/.test(path.basename(out))&&!fs.existsSync(out)&&fs.realpathSync.native(parent)===parent,'CONTACT_QT_FRESH_ROOT_OUTPUT_REQUIRED');
 fs.mkdirSync(out);const native=await import(pathToFileURL(root+'/scripts/healthroster-rehearsal-turn.mjs').href);let sequenceStart,localTiming={},callTiming,nativeTimes=[];
 try{
  const result=await native.withSharedRehearsalTurn({task:'CONTACT_QT_POSITIVE_'+caseId,progressPath:root+'/docs/healthroster-build/PROGRESS.md',
   beforePhase:async()=>{assertCommittedAdapter();await source(root,caseId);},async work(turn){
    sequenceStart=performance.now();localTiming.sequenceStartedAt=new Date().toISOString();localTiming.workDeadlineAt=new Date(Date.parse(localTiming.sequenceStartedAt)+BUDGET.workMilliseconds).toISOString();
    const deadline=new AbortController(),timer=setTimeout(()=>{localTiming.workTimerFiredAt=new Date().toISOString();deadline.abort(Error(WORK_DEADLINE_CODE));},BUDGET.workMilliseconds);
    const controls=new FiniteControls(process.stdin,AbortSignal.any([turn.signal,deadline.signal]));const owned={...turn,signal:AbortSignal.any([turn.signal,deadline.signal,controls.signal])};
    const within=()=>{if(owned.signal.aborted)throw owned.signal.reason;try{assertElapsed(sequenceStart,performance.now(),BUDGET.workMilliseconds);}catch{localTiming.workElapsedDeadlineObservedAt=new Date().toISOString();throw Error(WORK_DEADLINE_CODE);}};
    let beforePacket,afterPacket,nativeResult;
    try{
     within();const reader=await createFixedBatchMetadataReader({root,turn:owned,caseId});beforePacket=await reader.before();within();requireThat(beforePacket.childClosed===true,'CONTACT_QT_C_BEFORE_CLOSED_REQUIRED');
     const built=await buildInput({root,identity:turn.identity,caseId});requireThat(built.receipt.canonicalSqlSha256===reader.sourceDescriptor.referenceCanonicalSqlSha256,'CONTACT_QT_CANONICAL_QUERY_EQUALITY_REQUIRED');
     await turn.assertHeld('before-contact-qt-mcp-ready');within();saveSmall(out,'TURN_IDENTITY.json',turn.identity);saveSmall(out,'INPUT.json',built.input);saveSmall(out,'INPUT_RECEIPT.json',built.receipt);
     localTiming.readyAt=new Date().toISOString();saveSmall(out,'READY_TIMING.json',{...localTiming,sqlSha256:built.receipt.sqlSha256});const ready=controls.wait('PREPARE');
     console.log(JSON.stringify({status:'READY_ROOT_KIER_MCP_B_REQUIRED',caseId,out,route:ROUTE,programmeBytes:built.receipt.programmeBytes,readyAt:localTiming.readyAt,workDeadlineAt:localTiming.workDeadlineAt,remainingWorkMilliseconds:Math.floor(BUDGET.workMilliseconds-(performance.now()-sequenceStart)),canonicalMetadata:'PROCESS_MEMORY_ONLY'}));
     await ready;within();await turn.assertHeld('after-contact-qt-mcp-result');nativeResult=readSmall(out,'NATIVE_RESULT.json');localTiming.resultReadAt=new Date().toISOString();
     callTiming=rootCallTiming(readSmall(out,'NATIVE_CALL_TIMING.json'),{sqlSha256:built.receipt.sqlSha256,readyAt:localTiming.readyAt,resultReadAt:localTiming.resultReadAt});saveSmall(out,'ROOT_CALL_TIMING_VALIDATED.json',callTiming);
     nativeTimes=nativeClockFrames(nativeResult,built.receipt);saveSmall(out,'NATIVE_CLOCK_OBSERVATIONS.json',{status:'ACTUAL_CLOCK_FRAMES_ONLY_NO_SUCCESS_VERDICT',frames:nativeTimes,sqlSha256:built.receipt.sqlSha256});
     const stage=readStage(nativeResult,built.receipt);afterPacket=await reader.after(stage);within();const summary=await validateRestoration({root,result:nativeResult,receipt:built.receipt,beforePacket,afterPacket});within();
     const release=controls.wait('RELEASE');console.log(JSON.stringify({status:'QUALIFIED_PENDING_ROOT_RELEASE_AND_SHARED_TURN_CLOSE',caseId,out,route:ROUTE}));await release;within();return{...summary,rootCallTiming:callTiming,localTiming};
    }finally{clearTimeout(timer);controls.dispose();beforePacket=null;afterPacket=null;nativeResult=null;}
   }},{openTurn:()=>serialized(new native.OwnedTurnConnection(native.agencyChildEnvironment('healthroster_contact_qt_holder')))});
  if(sequenceStart===undefined){requireThat(['BUSY_NO_TRIAL','COOLDOWN_NO_CONNECTION'].includes(result?.status),'CONTACT_QT_SHARED_CALLBACK_NOT_RUN');const hold={...result,native:'NOT_RUN',completeMapsPersisted:false};saveSmall(out,'HOLD.json',hold);console.log(JSON.stringify(hold));return hold;}
  const elapsed=assertElapsed(sequenceStart),final={...result,status:'PASS_EXACT_CONTACT_QT_CASE_AND_ORIGINAL_CANONICAL_RESTORATION',elapsedMilliseconds:Math.ceil(elapsed),wholeSequenceBudgetMilliseconds:BUDGET.wholeMilliseconds,workBudgetMilliseconds:BUDGET.workMilliseconds,canonicalSharedTurnUnlockedAndClosed:true,completeMapsPersisted:false};
  saveSmall(out,'SUMMARY.json',final);console.log(JSON.stringify({status:final.status,caseId,elapsedMilliseconds:final.elapsedMilliseconds}));return final;
 }catch(error){const deadlineObserved=!!(localTiming.workTimerFiredAt||localTiming.workElapsedDeadlineObservedAt),code=deadlineObserved?WORK_DEADLINE_CODE:closed(error);
  saveSmall(out,'HOLD.json',{status:'HOLD',caseId,code,observedErrorCode:closed(error),deadlineObserved,localTiming,...metadataDiagnosticFor(error)?{metadataDiagnostic:metadataDiagnosticFor(error)}:{},...callTiming?{rootCallTiming:callTiming}:{},nativeClockFrames:nativeTimes,sharedTurnCompletion:'SEE_ORIGINAL_SEND7_JOURNAL_NO_SYNTHETIC_CLOSE_PROOF',dispatchedMcpMustBeReconciledBeforeNextTrial:true,elapsedMilliseconds:sequenceStart===undefined?null:Math.ceil(performance.now()-sequenceStart),completeMapsPersisted:false});throw Error(code);}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const[root,out,caseId,flag,...rest]=process.argv.slice(2);requireThat(flag==='--authorised-root-holder'&&rest.length===0,'CONTACT_QT_ROOT_HOLDER_AUTHORIZATION_REQUIRED');await holdCase({root,out,caseId,authorisedRootHolder:true});}
 catch(error){console.error(JSON.stringify({status:'HOLD',code:closed(error)}));process.exitCode=1;}
}
