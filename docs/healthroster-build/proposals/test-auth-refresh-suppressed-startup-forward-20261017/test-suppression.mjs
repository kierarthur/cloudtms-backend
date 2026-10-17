// Synthetic controls only: no browser, env file, credential/state IO or network.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import crypto from 'node:crypto';import vm from 'node:vm';import {fileURLToPath,pathToFileURL} from 'node:url';
import {ROOT,FRONT,BACK,networkGuard,requestKind,closedResult,ownedDirectory,check,PW} from './policy.mjs';
import {guardedRoute} from './worker.mjs';
import {networkGuard as priorGuard,requestKind as priorKind} from '../test-auth-refresh-diagnostic-forward-20261017/policy.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),ping=BACK+'/api/changes/ping';
test('one exact ping is still aborted without fetch/response, separately counted, second attempt holds',async()=>{
 const n=networkGuard();n.accept(BACK+'/auth/login','POST','fetch');let aborted=0;
 const route={request:()=>({url:()=>ping,method:()=> 'POST',resourceType:()=> 'fetch',postData:()=>{throw Error('BODY_MUST_NOT_BE_READ');}}),
  abort:async()=>aborted++,fetch:async()=>{throw Error('PING_MUST_NOT_BE_FORWARDED');},fulfill:async()=>{throw Error('NO_FAKE_RESPONSE');}};
 assert.equal(requestKind(ping,'POST','fetch'),'SUPPRESSED_STARTUP');await guardedRoute(route,n);assert.equal(aborted,1);n.assertAdmission();
 assert.equal(n.summary().suppressedStartupRequests,1);assert.equal(n.summary().forbiddenWriteRequests,0);assert.equal(n.diagnostics().blockedWriteLabel,'NONE');
 await guardedRoute(route,n);assert.equal(aborted,2);assert.equal(n.summary().suppressedStartupRequests,2);assert.throws(()=>n.assertAdmission());
});
test('only exact queryless normal-backend POST classification changes; every near miss is unchanged',()=>{
 const variants=[ping,ping+'?',ping+'?x=SYNTHETIC_PRIVATE',ping+'#SYNTHETIC_PRIVATE',ping+'/',ping.replace('/changes/','/Changes/'),
  ping.replace('/ping','/%70ing'),ping.replace('workers.dev/','workers.dev:443/'),FRONT+'/api/changes/ping','https://other.invalid/api/changes/ping',BACK+'/api/users/me/grid-prefs'];
 let changed=0;
 for(const url of variants)for(const method of ['GET','OPTIONS','POST','PATCH','PUT','DELETE','post']){
  const before=priorKind(url,method,'fetch'),after=requestKind(url,method,'fetch');
  if(before!==after){changed++;assert.equal(url,ping);assert.equal(method,'POST');assert.equal(before,'FORBIDDEN_WRITE');assert.equal(after,'SUPPRESSED_STARTUP');}
  const a=priorGuard(),b=networkGuard();assert.equal(a.accept(url,method,'fetch'),b.accept(url,method,'fetch'));
  if(url!==ping||method!=='POST'){const {suppressedStartupRequests,...same}=b.summary();assert.equal(suppressedStartupRequests,0);assert.deepEqual(same,a.summary());}
 }
 assert.equal(changed,1);
 for(const args of [[BACK+'/api/users/me/grid-prefs','PATCH'],[ping+'?unapproved=1','POST']]){
  const n=networkGuard();n.accept(BACK+'/auth/login','POST','fetch');n.accept(ping,'POST','fetch');n.accept(...args,'fetch');assert.throws(()=>n.assertAdmission());
 }
});
test('closed local V3 requires the actual nonnegative suppression counter',()=>{
 const n=networkGuard();const result={version:'TEST_AUTH_REFRESH_PRIVATE_RESULT_V3',status:'HOLD',code:'AUTH_NETWORK_BOUNDARY_HELD',browserClosed:true,...n.summary(),diagnostic:n.diagnostics()};
 assert.equal(closedResult(result).suppressedStartupRequests,0);
 for(const value of [undefined,null,-1,'1',Infinity]){const bad={...result,suppressedStartupRequests:value};assert.throws(()=>closedResult(bad));}
});

async function controlledWorker(late){
 let state=false,result,network,closed=0;
 const out=ROOT+'/.codex-tmp/test-auth-refresh-20261017-'+'a'.repeat(32);
 const processStub={env:{HEALTH_TEST_AUTH_OWNED_OUT:out},argv:['node','CONTROLLED_NOT_MAIN','--owned-auth-worker'],send:()=>{},cwd:()=>out,
  on:()=>{},removeListener:()=>{},disconnect:()=>{},exitCode:undefined};
 const publicFile=path.join(here,'worker.mjs');const context=vm.createContext({process:processStub,AbortController,setTimeout,clearTimeout});
 const fakeFs={realpathSync:{native:p=>p},existsSync:p=>p.endsWith('storage-state.json')&&state,
  readFileSync:()=>Buffer.from(JSON.stringify({files:[]})),lstatSync:()=>({isFile:()=>true,isSymbolicLink:()=>false}),
  unlinkSync:()=>{state=false;},writeFileSync:(p,text)=>{assert(p.endsWith('RESULT.json'));result=JSON.parse(text);}};
 const page={setDefaultTimeout:()=>{},setDefaultNavigationTimeout:()=>{}};
 const browser={newContext:async()=>({route:async()=>{},routeWebSocket:async()=>{},
  request:{get:async(url,options)=>{assert.equal(url,BACK+'/readyz');assert.equal(options.maxRedirects,0);return {status:()=>200,dispose:async()=>{}};}},
  newPage:async()=>page,pages:()=>[page]}),close:async()=>{closed++;if(late==='secondPing')network.accept(ping,'POST','fetch');if(late==='otherWrite')network.accept(BACK+'/api/users/me/grid-prefs','PATCH','fetch');}};
 const exports={
  'node:fs':{default:fakeFs},'node:path':{default:path},'node:crypto':{default:crypto},'node:url':{fileURLToPath},
  './policy.mjs':{ownedDirectory,check,networkGuard,PW,BACK},
  [PW]:{chromium:{executablePath:()=> 'D:/Development/Caches/ms-playwright/CONTROLLED_ONLY/chrome.exe',launch:async()=>browser}},
  './config.mjs':{default:{use:{baseURL:FRONT},viewport:{width:1366,height:768}}},
  './authenticate.mjs':{authenticate:async args=>{network=args.network;network.accept(BACK+'/auth/login','POST','fetch');network.accept(ping,'POST','fetch');
   network.recordAdmission({meStatus:200,actorUuid:true,admin:true,workspaceStatus:200,contract:true,queries:true});network.assertAdmission();state=true;}}
 };
 async function stub(id){assert.ok(exports[id],id);const values=exports[id];const m=new vm.SyntheticModule(Object.keys(values),function(){for(const[k,v]of Object.entries(values))this.setExport(k,v);},{context});await m.link(()=>{});await m.evaluate();return m;}
 const module=new vm.SourceTextModule(fs.readFileSync(publicFile,'utf8'),{context,
  initializeImportMeta:meta=>{meta.url=pathToFileURL(path.join(out,'worker.mjs')).href;},importModuleDynamically:id=>stub(id)});
 await module.link(id=>stub(id));await module.evaluate();await module.namespace.runWorker();return {state,result,closed,exitCode:processStub.exitCode};
}
test('actual complete worker keeps state only for one suppressed ping after confirmed close',async()=>{
 const good=await controlledWorker('none');assert.equal(good.state,true);assert.equal(good.result.status,'AUTH_REFRESHED');assert.equal(good.result.suppressedStartupRequests,1);assert.equal(good.closed,1);assert.equal(good.exitCode,0);closedResult(good.result);
 for(const late of ['secondPing','otherWrite']){const r=await controlledWorker(late);assert.equal(r.state,false);assert.equal(r.result.status,'HOLD');assert.equal(r.result.code,'AUTH_NETWORK_BOUNDARY_HELD');assert.equal(r.result.browserClosed,true);assert.equal(r.closed,1);assert.equal(r.exitCode,1);closedResult(r.result);}
});
