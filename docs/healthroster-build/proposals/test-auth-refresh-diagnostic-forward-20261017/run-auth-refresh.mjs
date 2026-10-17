// Source-only package: root must review and run outside every native interval.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawn,execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {ROOT,FRONTEND,ENV_FILE,SHARED_STATE,ownedDirectory,closedResult,check} from './policy.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
const git=(root,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,stdio:['ignore','pipe','ignore'],maxBuffer:8*1024*1024});
export function sourcePreflight(){
 check(process.versions.node==='24.15.0','AUTH_REVIEWED_NODE_REQUIRED');
 check(/^D:\/Development\/GitHub(?:\/|$)/.test(physical(process.cwd())),'AUTH_ACTUAL_D_CWD_REQUIRED');
 for(const root of [ROOT,FRONTEND]){
  check(physical(root)===root&&physical(git(root,['rev-parse','--show-toplevel']).toString().trim())===root,'AUTH_PHYSICAL_ROOT_REQUIRED');
  const common=git(root,['rev-parse','--git-common-dir']).toString().trim();
  check(physical(path.resolve(root,common))===(root===ROOT?'D:/Development/GitHub/cloudtms-backend/.git':FRONTEND+'/.git'),'AUTH_GIT_COMMON_REQUIRED');
  const directory=git(root,['rev-parse','--absolute-git-dir']).toString().trim();
  check(physical(directory)===(root===ROOT?'D:/Development/GitHub/cloudtms-backend/.git/worktrees/healthroster-install2-14102026':FRONTEND+'/.git'),'AUTH_GIT_DIRECTORY_REQUIRED');
 }
 const manifest=JSON.parse(fs.readFileSync(path.join(here,'MANIFEST.json'),'utf8'));
 const top=git(here,['rev-parse','--show-toplevel']).toString().trim();
 const relative=path.relative(top,here).replaceAll('\\','/');
 check(relative==='docs/healthroster-build/proposals/test-auth-refresh-diagnostic-forward-20261017','AUTH_SOURCE_LOCATION_REQUIRED');
 for(const r of manifest.files){
  const b=fs.readFileSync(path.join(here,r.file));
  check(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(git(here,['show','HEAD:'+relative+'/'+r.file]))&&b.equals(git(here,['show',':'+relative+'/'+r.file])),'AUTH_REVIEWED_COMMITTED_SOURCE_REQUIRED');
 }
 check(fs.readFileSync(path.join(here,'MANIFEST.json')).equals(git(here,['show','HEAD:'+relative+'/MANIFEST.json']))
  &&fs.readFileSync(path.join(here,'MANIFEST.json')).equals(git(here,['show',':'+relative+'/MANIFEST.json'])),'AUTH_REVIEWED_MANIFEST_REQUIRED');
 const pins=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_PINS.json'),'utf8'));
 for(const r of pins.sources){
  const b=fs.readFileSync(path.join(r.root,r.source));check(b.length===r.bytes&&sha(b)===r.sha256,'AUTH_EXISTING_HARNESS_SOURCE_CHANGED');
  const comparable=r.gitLineEndingRule?Buffer.from(b.toString().replaceAll('\r\n','\n')):b;
  check(comparable.equals(git(r.root,['show',r.commit+':'+r.source])),'AUTH_EXISTING_HARNESS_GIT_REQUIRED');
 }
 for(const r of pins.localRuntime){
  check(physical(r.file)===r.file,'AUTH_EXISTING_RUNTIME_PHYSICAL_REQUIRED');
  const b=fs.readFileSync(r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'AUTH_EXISTING_RUNTIME_CHANGED');
 }
 const schema=pins.querySchemaSource;
 check(physical(schema.root)===schema.root,'AUTH_HEALTH_SCHEMA_PHYSICAL_REQUIRED');
 check(sha(fs.readFileSync(path.join(schema.root,schema.file)))===schema.sha256
  &&sha(fs.readFileSync(path.join(here,schema.captured)))===schema.capturedSha256,'AUTH_HEALTH_SCHEMA_SOURCE_CHANGED');
 check(sha(git(schema.root,['show',schema.commit+':'+schema.file]))===schema.sha256,'AUTH_HEALTH_SCHEMA_GIT_REQUIRED');
 const routes=JSON.parse(fs.readFileSync(path.join(here,'STATIC_ROUTE_SOURCE.json'),'utf8'));
 check(sha(fs.readFileSync(path.join(FRONTEND,routes.file)))===routes.savedSha256
  &&git(FRONTEND,['rev-parse',routes.commit+':'+routes.file]).toString().trim()===routes.gitBlob,'AUTH_STATIC_ROUTE_SOURCE_CHANGED');
 // Presence, type and ignore metadata only. Parent never reads either private file.
 for(const file of [ENV_FILE,SHARED_STATE]){
  const s=fs.lstatSync(file);check(s.isFile()&&!s.isSymbolicLink(),'AUTH_APPROVED_PRIVATE_FILE_REQUIRED');
  check(physical(path.dirname(file))===FRONTEND+'/tests/e2e/.auth','AUTH_APPROVED_PRIVATE_DIRECTORY_REQUIRED');
  git(FRONTEND,['check-ignore','--quiet','--',path.relative(FRONTEND,file)]);
 }
 check(physical(ROOT+'/.codex-tmp')===ROOT+'/.codex-tmp','AUTH_PRIVATE_PARENT_REQUIRED');
 return manifest;
}
export function childEnvironment(out){
 const env={};
 for(const name of ['SystemRoot','WINDIR','ComSpec','PATH','PATHEXT','USERPROFILE','HOMEDRIVE','HOMEPATH','LOCALAPPDATA'])
  if(process.env[name]!==undefined)env[name]=process.env[name];
 return {...env,TEMP:path.join(out,'tmp'),TMP:path.join(out,'tmp'),HEALTH_TEST_AUTH_OWNED_OUT:out,
  PLAYWRIGHT_BROWSERS_PATH:'D:/Development/Caches/ms-playwright',DEBUG:'',PWDEBUG:'0',NODE_NO_WARNINGS:'1'};
}
export function aclChildEnvironment(executable,inherited=process.env){
 const env={...inherited};
 // Windows child env keys are case-insensitive; remove every inherited spelling.
 for(const key of Object.keys(env))if(key.toLowerCase()==='psmodulepath')delete env[key];
 env.PSModulePath=path.join(path.dirname(executable),'Modules');
 return env;
}
export async function runAuthRefresh(flags){
 check(flags.length===2&&flags[0]==='--authorised-root-auth-refresh'&&flags[1]==='--outside-native-interval','AUTH_ROOT_OUTSIDE_NATIVE_AUTHORITY_REQUIRED');
 sourcePreflight();
 const out=ownedDirectory(ROOT+'/.codex-tmp/test-auth-refresh-20261017-'+crypto.randomBytes(16).toString('hex'));
 check(!fs.existsSync(out),'AUTH_FRESH_DIRECTORY_REQUIRED');fs.mkdirSync(out,{mode:0o700});
 const system=path.join(process.env.SystemRoot,'System32');
 const aclShell=path.join(system,'WindowsPowerShell/v1.0/powershell.exe');
 try{
  execFileSync(aclShell,['-NoProfile','-NonInteractive','-File',path.join(here,'secure-owned.ps1'),'-OwnedPath',out],
   {windowsHide:true,stdio:'ignore',timeout:15000,env:aclChildEnvironment(aclShell)});
 }catch{
  return {status:'HOLD',code:'AUTH_PRIVATE_DIRECTORY_SETUP_HELD',phase:'PRIVATE_DIRECTORY_ACL',
   ownedDirectory:out,browserStarted:false,credentialLoaderStarted:false};
 }
 git(ROOT,['check-ignore','--quiet','--',path.relative(ROOT,path.join(out,'storage-state.json'))]);
 fs.mkdirSync(path.join(out,'tmp'),{mode:0o700});
 const files=[];
 for(const[from,to]of [['forward/authenticate.mjs','authenticate.mjs'],['forward/config.mjs','config.mjs'],
  ...['worker.mjs','policy.mjs','credentials-child.mjs','admission.mjs'].map(f=>[f,f])]){
  const bytes=fs.readFileSync(path.join(here,from));fs.writeFileSync(path.join(out,to),bytes,{flag:'wx',mode:0o600});files.push({file:to,sha256:sha(bytes)});
 }
 fs.writeFileSync(path.join(out,'RUN_SOURCE_MANIFEST.json'),JSON.stringify({files})+'\n',{flag:'wx',mode:0o600});
 const child=spawn(process.execPath,[path.join(out,'worker.mjs'),'--owned-auth-worker'],{cwd:out,windowsHide:true,
  env:childEnvironment(out),stdio:['ignore','ignore','ignore','ipc']});
 let timedOut=false,closed=false;
 const code=await new Promise(resolve=>{
  let killTimer,closeTimer;
  const timer=setTimeout(()=>{
   timedOut=true;try{child.send('ABORT');}catch{}
   killTimer=setTimeout(()=>{
    try{execFileSync(path.join(system,'taskkill.exe'),['/PID',String(child.pid),'/T','/F'],{windowsHide:true,stdio:'ignore',timeout:5000});}catch{}
    closeTimer=setTimeout(()=>resolve(null),5000);
   },10000);
  },90000);
  child.once('error',()=>{clearTimeout(timer);resolve(null);});
  child.once('close',value=>{closed=true;clearTimeout(timer);clearTimeout(killTimer);clearTimeout(closeTimer);resolve(value);});
 });
 const state=path.join(out,'storage-state.json');
 let result;
 if(closed&&!timedOut){
  try{const file=path.join(out,'RESULT.json'),s=fs.lstatSync(file);
   check(s.isFile()&&!s.isSymbolicLink()&&s.size<=4096,'AUTH_CLOSED_RESULT_REQUIRED');
   result=closedResult(JSON.parse(fs.readFileSync(file,'utf8')));
  }catch{}
 }
 if(timedOut||!closed||code!==0){
  if(closed&&fs.existsSync(state)){const s=fs.lstatSync(state);if(s.isFile()&&!s.isSymbolicLink())fs.unlinkSync(state);}
  return {status:'HOLD',code:timedOut?'AUTH_WORK_BUDGET_HELD':result?.status==='HOLD'?result.code:'AUTH_REFRESH_HELD',ownedDirectory:out,browserClosure:'SEE_CLOSED_CHILD_RESULT',diagnostic:result?.diagnostic??null};
 }
 check(result?.status==='AUTH_REFRESHED'&&result.code==='AUTH_REFRESHED'
  &&result.browserClosed===true&&result.loginRequests===1&&result.refreshRequests<=1
  &&result.forbiddenWriteRequests===0&&result.refusedRequests===0,'AUTH_CLOSED_RESULT_REQUIRED');
 check(fs.lstatSync(state).isFile()&&!fs.lstatSync(state).isSymbolicLink(),'AUTH_FRESH_STATE_REQUIRED');
 return {status:'AUTH_REFRESHED',statePath:state,browserClosed:true,sharedFrontendStateUntouched:true,diagnostic:result.diagnostic,
  qualification:'ME_ADMIN_AND_QUERY_WORKSPACE_ONLY_NOT_NATIVE_WPPP_OR_BUSINESS_CURRENTNESS'};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const r=await runAuthRefresh(process.argv.slice(2));console.log(JSON.stringify(r));if(r.status!=='AUTH_REFRESHED')process.exitCode=1;}
 catch{console.log(JSON.stringify({status:'HOLD',code:'AUTH_ROOT_INVOCATION_HELD'}));process.exitCode=1;}
}
