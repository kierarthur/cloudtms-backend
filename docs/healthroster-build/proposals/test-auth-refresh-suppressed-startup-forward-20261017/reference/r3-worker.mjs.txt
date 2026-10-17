// Root launcher copies this public source into one fresh private ignored directory.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {ownedDirectory,check,networkGuard,PW,BACK} from './policy.mjs';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const allowed=new Set(['AUTH_2FA_REQUIRED','AUTH_APPROVED_ENV_UNAVAILABLE','AUTH_ME_ADMIN_ADMISSION_HELD',
 'AUTH_QUERY_WORKSPACE_ADMISSION_HELD','AUTH_NETWORK_BOUNDARY_HELD','AUTH_FRESH_STATE_REQUIRED',
 'AUTH_READY_HELD','AUTH_WORK_BUDGET_HELD','AUTH_BROWSER_CLOSE_UNCONFIRMED']);
export async function guardedRoute(route,network){
 if(!network.accept(route.request().url(),route.request().method(),route.request().resourceType()))return route.abort();
 try{
  // No automatic redirect can move an admitted auth/read request to another URL.
  const response=await route.fetch({maxRedirects:0,timeout:15000});
  if(response.status()>=300&&response.status()<400){network.refuseTransport();await route.abort();}
  else await route.fulfill({response});
  await response.dispose();
 }catch{network.refuseTransport();await route.abort().catch(()=>{});}
}
export async function runWorker(){
 check(typeof process.send==='function'&&process.argv.slice(2).join(' ')==='--owned-auth-worker','AUTH_WORKER_ONLY');
 const out=ownedDirectory(process.env.HEALTH_TEST_AUTH_OWNED_OUT);
 check(fs.realpathSync.native(path.dirname(fileURLToPath(import.meta.url))).replaceAll('\\','/')===out
  &&fs.realpathSync.native(process.cwd()).replaceAll('\\','/')===out,'AUTH_RUNTIME_DIRECTORY_REQUIRED');
 const state=path.join(out,'storage-state.json');check(!fs.existsSync(state),'AUTH_FRESH_STATE_REQUIRED');
 const binding=JSON.parse(fs.readFileSync(path.join(out,'RUN_SOURCE_MANIFEST.json'),'utf8'));
 for(const r of binding.files)check(sha(fs.readFileSync(path.join(out,r.file)))===r.sha256,'AUTH_RUNTIME_SOURCE_CHANGED');
 let browser,success=false,code='AUTH_REFRESH_HELD',closed=false;const network=networkGuard();
 const deadline=new AbortController();const abort=()=>deadline.abort(Error('AUTH_WORK_BUDGET_HELD'));
 const timer=setTimeout(abort,80000);const onMessage=m=>{if(m==='ABORT')abort();};process.on('message',onMessage);
 let rejectAbort;
 const interrupted=new Promise((_,reject)=>{rejectAbort=()=>{void browser?.close().catch(()=>{});reject(Error('AUTH_WORK_BUDGET_HELD'));};deadline.signal.addEventListener('abort',rejectAbort,{once:true});});
 void interrupted.catch(()=>{});
 try{
  const {chromium}=await import(PW);const {default:config}=await import('./config.mjs');
  const executable=fs.realpathSync.native(chromium.executablePath()).replaceAll('\\','/');
  check(executable.startsWith('D:/Development/Caches/ms-playwright/'),'AUTH_EXISTING_D_BROWSER_REQUIRED');
  browser=await chromium.launch({headless:true,executablePath:executable,timeout:15000,
   downloadsPath:path.join(out,'downloads'),args:['--disable-logging','--disable-crash-reporter','--disable-breakpad']});
  const context=await browser.newContext({baseURL:config.use.baseURL,ignoreHTTPSErrors:false,serviceWorkers:'block',
   viewport:config.viewport,storageState:{cookies:[],origins:[]},acceptDownloads:false});
  await context.route('**/*',route=>guardedRoute(route,network));
  await context.routeWebSocket('**/*',socket=>socket.close());
  const ready=await context.request.get(BACK+'/readyz',{timeout:15000,maxRedirects:0});
  const readyStatus=ready.status();await ready.dispose();check(readyStatus===200,'AUTH_READY_HELD');
  const page=await context.newPage();page.setDefaultTimeout(15000);page.setDefaultNavigationTimeout(20000);
  // Browser launches before the credential loader can run; process.env is restored before any await.
  const {authenticate}=await import('./authenticate.mjs');
  await Promise.race([authenticate({page,network}),interrupted]);
  check(context.pages().length===1,'AUTH_NETWORK_BOUNDARY_HELD');network.assertAdmission();
  check(fs.lstatSync(state).isFile()&&!fs.lstatSync(state).isSymbolicLink(),'AUTH_FRESH_STATE_REQUIRED');
  success=true;code='AUTH_REFRESHED';
 }catch(error){code=allowed.has(error?.message)?error.message:'AUTH_REFRESH_HELD';}
 finally{
  clearTimeout(timer);deadline.signal.removeEventListener('abort',rejectAbort);process.removeListener('message',onMessage);
  let closeTimer;
  try{
   await Promise.race([browser?.close(),new Promise((_,reject)=>{closeTimer=setTimeout(()=>reject(Error('AUTH_BROWSER_CLOSE_UNCONFIRMED')),8000);})]);
   closed=true;
  }catch{success=false;code='AUTH_BROWSER_CLOSE_UNCONFIRMED';}finally{clearTimeout(closeTimer);}
  if(!success&&fs.existsSync(state)){
   const s=fs.lstatSync(state);check(s.isFile()&&!s.isSymbolicLink(),'AUTH_OWNED_STATE_CLEANUP_HELD');fs.unlinkSync(state);
  }
  const result={version:'TEST_AUTH_REFRESH_PRIVATE_RESULT_V2',status:success&&closed?'AUTH_REFRESHED':'HOLD',code,
   browserClosed:closed,...network.summary(),diagnostic:network.diagnostics()};
  fs.writeFileSync(path.join(out,'RESULT.json'),JSON.stringify(result)+'\n',{flag:'wx',mode:0o600});
  process.exitCode=success&&closed?0:1;process.disconnect?.();
 }
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{await runWorker();}catch{process.exitCode=1;process.disconnect?.();}
}
