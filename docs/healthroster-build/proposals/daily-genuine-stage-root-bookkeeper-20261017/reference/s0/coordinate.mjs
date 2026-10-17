// Root-only S0; no native/provider transport. Imported modules create no browser, child or IPC.
import fs from 'node:fs';import path from 'node:path';import {pathToFileURL,fileURLToPath} from 'node:url';
import {ROOT,check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {Controls,BUDGET} from '../daily-health-s0-catalogue-holder-20261017/protocol.mjs';
import {boundedConnection} from '../daily-health-s0-catalogue-holder-20261017/hold.mjs';
import {protectedRelease} from '../daily-health-s0-catalogue-holder-20261017/source.mjs';
import {source} from './source.mjs';import {runPhase} from './phase.mjs';import {s0Sequence,EFFECT,AFTER_EFFECT} from './sequence.mjs';
export async function coordinate(){
 const started=performance.now(),deadline=Date.now()+BUDGET.work,abort=new AbortController();let out,complete=false,sourceCommit;
 const requestAbort=()=>abort.abort(Error('S0_STAGE_ROOT_ABORT'));process.once('SIGINT',requestAbort);process.once('SIGTERM',requestAbort);
 const workTimer=setTimeout(()=>abort.abort(Error('S0_STAGE_WORK_BUDGET_HELD')),BUDGET.work),wholeTimer=setTimeout(()=>abort.abort(Error('S0_STAGE_WHOLE_BUDGET_HELD')),BUDGET.whole);
 const within=()=>check(!abort.signal.aborted&&Date.now()<deadline&&performance.now()-started<BUDGET.work,'S0_STAGE_WORK_BUDGET_HELD');
 try{
  sourceCommit=source();within();
  const native=await import(pathToFileURL(ROOT+'/scripts/healthroster-rehearsal-turn.mjs').href),controlOwner=await import(pathToFileURL(ROOT+'/scripts/healthroster-turn-control.mjs').href);
  const result=await native.withSharedRehearsalTurn({task:'DAILY_GENUINE_S0_STAGED_ONLY',progressPath:ROOT+'/docs/healthroster-build/PROGRESS.md',beforePhase:async()=>{source(sourceCommit);within();},async work(turn){
   const signal=AbortSignal.any([turn.signal,abort.signal]),controls=new Controls(process.stdin,signal);let control;
   try{control=await controlOwner.serveTurnControl({identity:turn.identity,signal,assertHeld:turn.assertHeld});
    const value=await s0Sequence({turn,controls,signal,within,phase:async phase=>{const r=await runPhase({phase,out,controlEnvironment:control.environment,signal,assertHeld:turn.assertHeld,runOwnedTurnChild:controlOwner.runOwnedTurnChild});out=r.ownedDirectory;return r;},
     announce:status=>console.log(JSON.stringify({status,stage:'S0',ownedDirectory:out,operatorSourceCommit:sourceCommit,remainingWorkMs:Math.max(0,deadline-Date.now()),
      beforeInput:'S0_BEFORE_INPUT.json',beforeResult:'S0_BEFORE_NATIVE.json',setupEntry:'S0_SETUP_ENTRY.md',afterInput:'S0_AFTER_INPUT.json',afterResult:'S0_AFTER_NATIVE.json',effectFile:EFFECT,afterEffectFile:AFTER_EFFECT,businessRequests:0}))});
    complete=true;clearTimeout(workTimer);return value;
   }finally{controls.dispose();await control?.close();}
  }},{checkProtectedRelease:()=>{within();protectedRelease();within();},openTurn:()=>{within();return boundedConnection(new native.OwnedTurnConnection(native.agencyChildEnvironment('healthroster_genuine_s0_staged'),{probeMs:5000}));}});
  check(performance.now()-started<BUDGET.whole,'S0_STAGE_WHOLE_BUDGET_HELD');
  const summary={status:complete?'S0_NATIVE_QUALIFIED_OWN_TURN_RELEASED':result.status,ownedDirectory:out??null,operatorSourceCommit:sourceCommit,stage:'S0',businessRequests:0,elapsedMs:Math.ceil(performance.now()-started),publicComposer:'NOT_PROVIDED',financialSuccess:'NOT_CLAIMED',sharedTurn:'ORIGINAL_EXPLICIT_UNLOCK_CLOSE_REQUIRED'};
  if(out)fs.writeFileSync(out+'/S0_COORDINATOR_SUMMARY.json',JSON.stringify(summary)+'\n',{flag:'wx',mode:0o600});console.log(JSON.stringify(summary));return summary;
 }catch(error){const code=/^[A-Z][A-Z0-9_]+$/.test(error?.message??'')?error.message:'S0_STAGE_COORDINATOR_HELD';const hold={status:'HOLD',code,ownedDirectory:out??null,stage:'S0',businessRequests:0,sourceCommit:sourceCommit??null,qualifiedPrefix:'READ_ACTUAL_CLOSED_CHILD_RECORDS_NO_SYNTHETIC_VERDICT',sharedTurn:'SEE_ORIGINAL_SEND7_RELEASE_RECORD'};if(out)try{fs.writeFileSync(out+'/S0_COORDINATOR_HOLD.json',JSON.stringify(hold)+'\n',{flag:'wx',mode:0o600});}catch{}console.log(JSON.stringify(hold));throw Error(code);
 }finally{clearTimeout(workTimer);clearTimeout(wholeTimer);process.removeListener('SIGINT',requestAbort);process.removeListener('SIGTERM',requestAbort);}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){if(process.argv.length!==3||process.argv[2]!=='--authorised-root-s0-staged'){console.log(JSON.stringify({status:'HOLD',code:'S0_STAGE_ROOT_AUTHORITY_REQUIRED'}));process.exitCode=1;}else try{await coordinate();}catch{process.exitCode=1;}}
