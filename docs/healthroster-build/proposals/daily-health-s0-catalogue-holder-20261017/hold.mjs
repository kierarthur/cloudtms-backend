// Root-only catalogue metadata holder. Importing creates no connection, child, auth or IPC.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
import {ROOT,check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {aclChildEnvironment} from '../test-auth-refresh-suppressed-startup-forward-20261017/run-auth-refresh.mjs';
import {buildS0CatalogueInput} from '../daily-health-genuine-staged-preflight-batch-forward-20261017/s0-scope.mjs';
import {source,protectedRelease,here} from './source.mjs';import {BUDGET,Controls,catalogueWork,save,nativeResult} from './protocol.mjs';
const closed=e=>/^[A-Z][A-Z0-9_]+$/.test(e?.message??'')?e.message:'S0_CATALOGUE_HELD';
export function boundedConnection(connection,{set=setTimeout,clear=clearTimeout}={}){
 const query=connection.query.bind(connection);let queue=Promise.resolve();connection.query=sql=>{const p=queue.then(()=>query(sql));queue=p.catch(()=>{});return p;};
 const originalClose=connection.close.bind(connection);connection.close=async()=>{let timer,killTimer;const done=originalClose();try{await Promise.race([done,new Promise((_,reject)=>{timer=set(()=>{if(!connection.closed)connection.child.kill('SIGKILL');killTimer=set(()=>reject(Error('S0_OWN_A_CLOSE_UNCONFIRMED')),5000);},10000);})]);check(connection.closed===true,'S0_OWN_A_CLOSE_UNCONFIRMED');}finally{clear(timer);clear(killTimer);}};return connection;
}
export async function hold(){
 const start=performance.now(),deadline=Date.now()+BUDGET.work,abort=new AbortController();let out,workStarted=false;
 const requestAbort=()=>abort.abort(Error('S0_ROOT_ABORT'));process.once('SIGTERM',requestAbort);process.once('SIGINT',requestAbort);
 const wholeTimer=setTimeout(()=>abort.abort(Error('S0_WHOLE_BUDGET_HELD')),BUDGET.whole),workTimer=setTimeout(()=>abort.abort(Error('S0_WORK_BUDGET_HELD')),BUDGET.work);
 const within=()=>check(!abort.signal.aborted&&Date.now()<deadline&&performance.now()-start<BUDGET.work,'S0_WORK_BUDGET_HELD');
 try{
  source();within();const parent=ROOT+'/.codex-tmp';check(fs.realpathSync.native(parent).replaceAll('\\','/')===parent,'S0_PHYSICAL_OUTPUT_PARENT_REQUIRED');out=parent+'/daily-s0-catalogue-20261017-'+crypto.randomBytes(16).toString('hex');check(!fs.existsSync(out),'S0_FRESH_OUTPUT_REQUIRED');fs.mkdirSync(out,{mode:0o700});
  const ps=path.join(process.env.SystemRoot,'System32/WindowsPowerShell/v1.0/powershell.exe');execFileSync(ps,['-NoProfile','-NonInteractive','-File',here+'/secure-owned.ps1','-OwnedPath',out],{windowsHide:true,stdio:'ignore',timeout:15000,env:aclChildEnvironment(ps)});within();
  const native=await import(pathToFileURL(ROOT+'/scripts/healthroster-rehearsal-turn.mjs').href);
  const result=await native.withSharedRehearsalTurn({task:'DAILY_S0_CATALOGUE_ONLY_CAPTURE',progressPath:ROOT+'/docs/healthroster-build/PROGRESS.md',beforePhase:async()=>{source();within();},
   async work(turn){workStarted=true;const signal=AbortSignal.any([turn.signal,abort.signal]),controls=new Controls(process.stdin,signal);
    try{return await catalogueWork({turn,signal,controls,build:buildS0CatalogueInput,within,
     saveInput:b=>{save(out,'INPUT.json',b.input);save(out,'INPUT_RECEIPT.json',b.receipt);},readResult:()=>nativeResult(out),saveReceipt:r=>save(out,'CAPTURE_RECEIPT.json',r),
     announce:status=>console.log(JSON.stringify({status,ownedDirectory:out,inputFile:'INPUT.json',nativeResultFile:'NATIVE_RESULT.json',remainingWorkMs:Math.max(0,deadline-Date.now()),compatibility:'ROOT_REVIEW_REQUIRED',businessScope:'NOT_RUN'}))});
    }finally{controls.dispose();}
   }},{checkProtectedRelease:()=>{within();protectedRelease();within();},openTurn:()=>{within();return boundedConnection(new native.OwnedTurnConnection(native.agencyChildEnvironment('healthroster_s0_catalogue_holder'),{probeMs:5000}));}});
  if(!workStarted){const r={status:result.status,native:'NOT_RUN',ownedDirectory:out};save(out,'HOLD.json',r);console.log(JSON.stringify(r));return r;}
  check(performance.now()-start<=BUDGET.whole,'S0_WHOLE_BUDGET_HELD');const summary={status:'CATALOGUE_CAPTURED_OWN_TURN_RELEASED',transport:result,compatibility:'ROOT_REVIEW_REQUIRED_NOT_QUALIFIED',businessScope:'NOT_RUN',originalSharedTurnCompleted:true,elapsedMs:Math.ceil(performance.now()-start),budgets:BUDGET};save(out,'SUMMARY.json',summary);console.log(JSON.stringify({status:summary.status,ownedDirectory:out,businessScope:'NOT_RUN',compatibility:summary.compatibility}));return summary;
 }catch(error){const r={status:'HOLD',code:closed(error),ownedDirectory:out??null,workStarted,elapsedMs:Math.ceil(performance.now()-start),sharedTurnProof:'SEE_ORIGINAL_SEND7_JOURNAL_NO_SYNTHETIC_RELEASE',compatibility:'NOT_QUALIFIED',businessScope:'NOT_RUN'};if(out)try{save(out,'HOLD.json',r);}catch{}console.log(JSON.stringify(r));throw Error(r.code);
 }finally{clearTimeout(wholeTimer);clearTimeout(workTimer);process.removeListener('SIGTERM',requestAbort);process.removeListener('SIGINT',requestAbort);}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){if(process.argv.length!==3||process.argv[2]!=='--authorised-root-s0-catalogue'){console.log(JSON.stringify({status:'HOLD',code:'S0_ROOT_AUTHORITY_REQUIRED'}));process.exitCode=1;}else try{await hold();}catch{process.exitCode=1;}}
