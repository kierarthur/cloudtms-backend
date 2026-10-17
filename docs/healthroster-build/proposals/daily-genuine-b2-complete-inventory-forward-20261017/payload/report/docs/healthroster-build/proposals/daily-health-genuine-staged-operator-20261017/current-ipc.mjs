// One closed B2 exchange over the already-owned child IPC and original root Controls.
// No new socket, transport, native executor, browser or timeout/budget.
import {check,exact} from './contract.mjs';
import {assertCurrentReady,currentFiles} from './current-read.mjs';
export async function waitCurrentResume(channel,ready,{signal,onViolation}){
 assertCurrentReady(ready);check(!signal.aborted,'FIXTURE_CURRENT_WAIT_ABORTED');
 let settled=false,failed=false,resolve,reject;
 const cleanup=()=>{channel.removeListener('message',message);channel.removeListener('disconnect',disconnect);signal.removeEventListener('abort',aborted);};
 const fail=()=>{if(failed)return;failed=true;onViolation();if(!settled){settled=true;cleanup();reject(Error('FIXTURE_CURRENT_IPC_REFUSED'));}};
 const message=m=>{if(m==='ABORT'){fail();return;}try{
  check(!settled&&exact(m,['type','nonce','evidenceFile','bindingSha256'])&&m.type==='CURRENT_NATIVE_SAVED'&&m.nonce===ready.binding.nonce
   &&m.bindingSha256===ready.bindingSha256&&m.evidenceFile===currentFiles(ready.binding.stage).evidence,'FIXTURE_CURRENT_IPC_REFUSED');
  settled=true;resolve({evidenceFile:m.evidenceFile,dispose:cleanup});
 }catch{fail();}};
 const disconnect=()=>fail(),aborted=()=>fail();
 const waiting=new Promise((a,b)=>{resolve=a;reject=b;});
 channel.on('message',message);channel.on('disconnect',disconnect);signal.addEventListener('abort',aborted,{once:true});
 try{channel.send(ready,error=>{if(error)fail();});}catch{fail();}
 return waiting; // The watcher stays active through POST: duplicate/foreign resumes abort the phase.
}
export function parentCurrentBridge({stage,source,controls,announce,assertHeld,signal}){
 let child,seen=false,resumed=false,failure,closed=false,listener;
 const refuse=()=>{failure??=Error('FIXTURE_CURRENT_PARENT_IPC_REFUSED');controls.fail('FIXTURE_CURRENT_PARENT_IPC_REFUSED');try{child?.send('ABORT',()=>{});}catch{}};
 return {
  attach(actualChild){check(!child,'FIXTURE_CURRENT_SINGLE_CHILD_REQUIRED');child=actualChild;
   listener=ready=>{try{check(!seen&&!closed&&!signal.aborted,'FIXTURE_CURRENT_PARENT_IPC_REFUSED');assertCurrentReady(ready,{stage,source});seen=true;}catch{refuse();return;}
    void (async()=>{
     await assertHeld('daily-genuine-'+stage+'-current-ready');check(!closed&&!signal.aborted,'FIXTURE_CURRENT_PARENT_IPC_REFUSED');controls.assertActive();
     const saved=controls.wait('CURRENT_NATIVE_SAVED');announce(ready);await saved;
     controls.assertActive();await assertHeld('daily-genuine-'+stage+'-current-saved');check(!closed&&!signal.aborted,'FIXTURE_CURRENT_PARENT_IPC_REFUSED');
     await new Promise((resolve,reject)=>{try{child.send({type:'CURRENT_NATIVE_SAVED',nonce:ready.binding.nonce,evidenceFile:currentFiles(stage).evidence,bindingSha256:ready.bindingSha256},error=>error?reject(error):resolve());}catch(error){reject(error);}});resumed=true;
    })().catch(refuse);
   };child.on('message',listener);
  },
  close(){closed=true;child?.removeListener('message',listener);if(seen&&!resumed)refuse();},
  assertCompleted(){check(!failure&&seen&&resumed,'FIXTURE_CURRENT_PARENT_EXCHANGE_REQUIRED');},
 };
}
