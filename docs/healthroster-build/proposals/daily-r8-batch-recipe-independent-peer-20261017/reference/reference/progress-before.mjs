// Closed existing coordinator stdout only. No private binding/state, IPC, auth, native or control injection.
import fs from 'node:fs';import path from 'node:path';import {setTimeout as delay} from 'node:timers/promises';import {fileURLToPath} from 'node:url';
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',ready=new Set(['BEFORE_READY','SETUP_ENTRY_READY','AFTER_READY','S0_QUALIFIED_PENDING_RELEASE']);
const fail=code=>{throw Error(code);},check=(v,c)=>{if(!v)fail(c);};
const uuidPath=p=>typeof p==='string'&&p.startsWith(ROOT+'/.codex-tmp/daily-genuine-stages-20261017-')&&/\/daily-genuine-stages-20261017-[a-f0-9]{32}$/.test(p);
export function scan(bytes,{expected,sourceCommit,ownedDirectory=null}){
 check(Buffer.isBuffer(bytes)&&bytes.length<=16384&&ready.has(expected)&&/^[a-f0-9]{40}$/.test(sourceCommit),'PROGRESS_CLOSED_INPUT_REQUIRED');
 const text=new TextDecoder('utf-8',{fatal:true}).decode(bytes),lines=text.split('\n');lines.pop();let found=null;
 for(let line of lines){line=line.replace(/\r$/,'');check(line.length>0,'PROGRESS_UNKNOWN_RECORD');const r=JSON.parse(line);check(r&&typeof r==='object'&&!Array.isArray(r),'PROGRESS_UNKNOWN_RECORD');
  if(r.status==='HOLD')fail('PROGRESS_ACTUAL_HOLD');
  check(ready.has(r.status)&&Object.keys(r).sort().join(',')==='afterEffectFile,afterInput,afterResult,beforeInput,beforeResult,businessRequests,effectFile,operatorSourceCommit,ownedDirectory,remainingWorkMs,setupEntry,stage,status'
   &&r.stage==='S0'&&r.operatorSourceCommit===sourceCommit&&uuidPath(r.ownedDirectory)&&(!ownedDirectory||r.ownedDirectory===ownedDirectory)&&Number.isSafeInteger(r.remainingWorkMs)&&r.remainingWorkMs>0&&r.remainingWorkMs<=120000&&r.businessRequests===0
   &&r.beforeInput==='S0_BEFORE_INPUT.json'&&r.beforeResult==='S0_BEFORE_NATIVE.json'&&r.afterInput==='S0_AFTER_INPUT.json'&&r.afterResult==='S0_AFTER_NATIVE.json'&&r.setupEntry==='S0_SETUP_ENTRY.md'
   &&r.effectFile==='docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json'&&r.afterEffectFile==='docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_S0_AFTER_EFFECT_QUALIFICATION_20261017.json','PROGRESS_UNKNOWN_OR_WRONG_SOURCE_RECORD');
  if(r.status===expected){check(!found,'PROGRESS_COLLISION');found=r;}
 }
 return found?{status:'ACTUAL_READY_OBSERVED',ready:found.status,ownedDirectory:found.ownedDirectory,operatorSourceCommit:found.operatorSourceCommit,remainingWorkMs:found.remainingWorkMs}:null;
}
export async function waitProgress(file,options,{timeoutMs=5000,read=()=>{const s=fs.lstatSync(file);check(s.isFile()&&!s.isSymbolicLink()&&s.size<=16384,'PROGRESS_FILE_REQUIRED');return {bytes:fs.readFileSync(file),identity:[s.dev,s.ino,s.birthtimeMs].join(':'),born:s.birthtimeMs};},now=Date.now,pause=delay}={}){
 const start=now();let identity,previous=Buffer.alloc(0);
 while(now()-start<timeoutMs){const value=read();check(value.born>=options.createdAfterMs-1000,'PROGRESS_STALE_FILE');if(identity!==undefined)check(value.identity===identity&&value.bytes.subarray(0,previous.length).equals(previous),'PROGRESS_REPLACED_OR_TRUNCATED');identity=value.identity;previous=value.bytes;
  const ready=scan(value.bytes,options);if(ready)return ready;await pause(50);
 }fail('PROGRESS_READY_TIMEOUT');
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const [file,expected,sourceCommit,created,ownedDirectory]=process.argv.slice(2),parent=path.dirname(file??'').replaceAll('\\','/');check(process.argv.length===7&&path.basename(file)==='PROGRESS.jsonl'&&path.posix.dirname(parent)===ROOT+'/.codex-tmp'&&/^daily-genuine-progress-20261017-[a-f0-9]{32}$/.test(path.posix.basename(parent))&&fs.realpathSync.native(parent).replaceAll('\\','/')===parent,'PROGRESS_ROOT_OWNED_PATH_REQUIRED');const createdAfterMs=Number(created);check(Number.isSafeInteger(createdAfterMs)&&createdAfterMs>0,'PROGRESS_CREATION_BOUND_REQUIRED');check(ownedDirectory==='NEW_BIND'||uuidPath(ownedDirectory),'PROGRESS_OWNED_STAGE_REQUIRED');const result=await waitProgress(file,{expected,sourceCommit,createdAfterMs,ownedDirectory:ownedDirectory==='NEW_BIND'?null:ownedDirectory});console.log(JSON.stringify(result));}catch(error){console.log(JSON.stringify({status:'HOLD',code:/^PROGRESS_[A-Z0-9_]+$/.test(error?.message??'')?error.message:'PROGRESS_CLOSED_READ_HELD'}));process.exitCode=1;}
}
