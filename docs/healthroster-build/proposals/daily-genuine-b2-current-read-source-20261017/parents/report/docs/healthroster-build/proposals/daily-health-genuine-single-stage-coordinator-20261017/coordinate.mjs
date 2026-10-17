// Root-only one-stage continuation. No native/provider transport and no new session binding.
import fs from 'node:fs';import path from 'node:path';import {pathToFileURL,fileURLToPath} from 'node:url';
import {ROOT,check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {Controls,BUDGET} from '../daily-health-s0-catalogue-holder-20261017/protocol.mjs';
import {boundedConnection} from '../daily-health-s0-catalogue-holder-20261017/hold.mjs';
import {protectedRelease} from '../daily-health-s0-catalogue-holder-20261017/source.mjs';
import {source} from './source.mjs';import {runPhase,selection} from './stage.mjs';import {oneStageSequence} from './sequence.mjs';
export async function coordinate(flags){
 const selected=selection(flags),{stage,out}=selected,started=performance.now(),deadline=Date.now()+BUDGET.work,abort=new AbortController();let complete=false,sourceCommit,outVerified=false,businessRequests=0;
 const requestAbort=()=>abort.abort(Error('GENUINE_STAGE_ROOT_ABORT'));process.once('SIGINT',requestAbort);process.once('SIGTERM',requestAbort);
 const workTimer=setTimeout(()=>abort.abort(Error('GENUINE_STAGE_WORK_BUDGET_HELD')),BUDGET.work),wholeTimer=setTimeout(()=>abort.abort(Error('GENUINE_STAGE_WHOLE_BUDGET_HELD')),BUDGET.whole);
 const within=()=>check(!abort.signal.aborted&&Date.now()<deadline&&performance.now()-started<BUDGET.work,'GENUINE_STAGE_WORK_BUDGET_HELD');
 try{
  sourceCommit=source();within();
  const native=await import(pathToFileURL(ROOT+'/scripts/healthroster-rehearsal-turn.mjs').href),controlOwner=await import(pathToFileURL(ROOT+'/scripts/healthroster-turn-control.mjs').href);
  const result=await native.withSharedRehearsalTurn({task:'DAILY_GENUINE_'+stage+'_STAGED_ONLY',progressPath:ROOT+'/docs/healthroster-build/PROGRESS.md',beforePhase:async()=>{source(sourceCommit);within();},async work(turn){
   const signal=AbortSignal.any([turn.signal,abort.signal]),controls=new Controls(process.stdin,signal);let control;
   const report=status=>console.log(JSON.stringify({status,stage,ownedDirectory:out,operatorSourceCommit:sourceCommit,remainingWorkMs:Math.max(0,deadline-Date.now()),beforeInput:stage+'_BEFORE_INPUT.json',beforeResult:stage+'_BEFORE_NATIVE.json',setupEntry:stage+'_SETUP_ENTRY.md',afterInput:stage+'_AFTER_INPUT.json',afterResult:stage+'_AFTER_NATIVE.json',effectFile:selected.effect,afterEffectFile:selected.afterEffect,businessRequests}));
   try{control=await controlOwner.serveTurnControl({identity:turn.identity,signal,assertHeld:turn.assertHeld});
    const value=await oneStageSequence({stage,turn,controls,signal,within,phase:async phase=>{const r=await runPhase({phase,stage,out,controlEnvironment:control.environment,signal,assertHeld:turn.assertHeld,runOwnedTurnChild:controlOwner.runOwnedTurnChild});check(r.ownedDirectory===out,'GENUINE_STAGE_RETAINED_SESSION_REQUIRED');outVerified=r.childClosed===true;if(phase==='submit'&&r.status==='PASS')businessRequests=1;return r;},announce:report,held:r=>console.log(JSON.stringify({status:'STAGE_HELD_RECONCILIATION_REQUIRED',...r,ownedDirectory:out,retry:'NEVER_AUTOMATIC'}))});
    complete=true;clearTimeout(workTimer);return value;
   }finally{controls.dispose();await control?.close();}
  }},{checkProtectedRelease:()=>{within();protectedRelease();within();},openTurn:()=>{within();return boundedConnection(new native.OwnedTurnConnection(native.agencyChildEnvironment('healthroster_genuine_'+stage.toLowerCase()+'_staged'),{probeMs:5000}));}});
  check(performance.now()-started<BUDGET.whole,'GENUINE_STAGE_WHOLE_BUDGET_HELD');
  const summary={status:complete?'GENUINE_STAGE_NATIVE_QUALIFIED_OWN_TURN_RELEASED':result.status,ownedDirectory:out,operatorSourceCommit:sourceCommit,stage,oneBusinessRequestQualified:complete,elapsedMs:Math.ceil(performance.now()-started),publicComposer:'NOT_PROVIDED',financialSuccess:'NOT_CLAIMED',sharedTurn:'ORIGINAL_EXPLICIT_UNLOCK_CLOSE_REQUIRED'};
  fs.writeFileSync(out+'/'+stage+'_COORDINATOR_SUMMARY.json',JSON.stringify(summary)+'\n',{flag:'wx',mode:0o600});console.log(JSON.stringify(summary));return summary;
 }catch(error){const code=/^[A-Z][A-Z0-9_]+$/.test(error?.message??'')?error.message:'GENUINE_STAGE_COORDINATOR_HELD';const hold={status:'HOLD',code,ownedDirectory:out,stage,sourceCommit:sourceCommit??null,attempt:'MAY_EXIST_RECONCILE_ACTUAL_CHILD_RECORDS',retry:'NEVER_AUTOMATIC',sharedTurn:'SEE_ORIGINAL_SEND7_RELEASE_RECORD'};if(outVerified)try{fs.writeFileSync(out+'/'+stage+'_COORDINATOR_HOLD.json',JSON.stringify(hold)+'\n',{flag:'wx',mode:0o600});}catch{}console.log(JSON.stringify(hold));throw Error(code);
 }finally{clearTimeout(workTimer);clearTimeout(wholeTimer);process.removeListener('SIGINT',requestAbort);process.removeListener('SIGTERM',requestAbort);}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){try{await coordinate(process.argv.slice(2));}catch{process.exitCode=1;}}
