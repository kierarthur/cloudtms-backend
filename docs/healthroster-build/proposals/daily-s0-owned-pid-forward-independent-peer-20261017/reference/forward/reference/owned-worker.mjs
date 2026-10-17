// Adapts only the captured child object to the existing Windows task-owned tree primitive.
// No process search, remote cancellation, arbitrary PID or browser launch occurs on import.
import path from 'node:path';import {spawn,execFile} from 'node:child_process';
import {check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
export function ownedWorker({spawnChild=spawn,set=setTimeout,clear=clearTimeout,killTree=(pid)=>new Promise((resolve,reject)=>execFile(path.join(process.env.SystemRoot,'System32/taskkill.exe'),['/PID',String(pid),'/T','/F'],{windowsHide:true,stdio:'ignore',timeout:5000,maxBuffer:65536},error=>error?reject(Error('S0_STAGE_OWN_TREE_TERMINATION_HELD')):resolve()))}={}){
 let child,closed=false,closing=false,forced=null,forceFailed=false,exitCode=null,resolveClose;const actualClose=new Promise(r=>resolveClose=r);
 const force=()=>{if(closed)return;if(!child||!Number.isSafeInteger(child.pid)||child.pid<1||child.exitCode!==null||child.signalCode!==null){forceFailed=true;return;}forced??=Promise.resolve().then(()=>killTree(child.pid)).catch(()=>{forceFailed=true;});};
 return {
  spawn(file,args,options){check(!child,'S0_STAGE_SINGLE_OWNED_CHILD_REQUIRED');child=spawnChild(file,args,options);
   // Never SIGTERM the launcher and orphan its worker. The direct worker receives ABORT first.
   child.kill=signal=>{if(closed)return false;closing=true;if(signal==='SIGKILL')force();else try{child.send('ABORT',()=>{});}catch{force();}return true;};
   child.once('close',code=>{closed=true;exitCode=code;resolveClose();});return child;
  },
  async wait(operation,signal){let timer;const onAbort=()=>{timer??=set(()=>rejectBound(Error('S0_STAGE_OWN_WORKER_CLOSE_UNCONFIRMED')),15000);};let rejectBound;const bound=new Promise((_,reject)=>rejectBound=reject);signal.addEventListener('abort',onAbort,{once:true});if(signal.aborted)onAbort();try{return await Promise.race([operation,bound]);}finally{signal.removeEventListener('abort',onAbort);clear(timer);}},
  async confirm(){if(!child)return {childClosed:true,started:false};if(forced)await forced;check(!forceFailed,'S0_STAGE_OWN_TREE_CLOSE_UNCONFIRMED');let timer;try{if(!closed)await Promise.race([actualClose,new Promise((_,reject)=>{timer=set(()=>reject(Error('S0_STAGE_OWN_TREE_CLOSE_UNCONFIRMED')),5000);})]);}finally{clear(timer);}check(closed,'S0_STAGE_OWN_TREE_CLOSE_UNCONFIRMED');return {childClosed:true,started:true,cooperativeAbort:closing,forcedTree:forced!==null,exitCode};},
  async stop(){if(forced)await forced;return {failed:!closed||forceFailed,code:'S0_STAGE_OWN_TREE_CLOSE_UNCONFIRMED'};},
  start(){},
 };
}
