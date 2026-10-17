// Offline synthetic controls only: no browser, network, env file or saved state.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import {ROOT,FRONT,BACK,ME,WORKSPACE,requestKind,networkGuard,ownedDirectory,closedResult,check} from './policy.mjs';
import {childEnvironment,runAuthRefresh} from './run-auth-refresh.mjs';
import {runWorker,guardedRoute} from './worker.mjs';
import {admission} from './admission.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const out=ROOT+'/.codex-tmp/test-auth-refresh-20261017-'+'a'.repeat(32);

test('fixed normal TEST auth/read routes and original static origins only',()=>{
 for(const [u,m,t,k]of [[ME,'GET','fetch','ME'],[WORKSPACE,'GET','fetch','WORKSPACE'],[BACK+'/auth/login','POST','fetch','LOGIN'],
  [BACK+'/auth/refresh','POST','fetch','REFRESH'],[FRONT+'/','GET','document','HOME'],[FRONT+'/js/main.js?v=1','GET','script','STATIC']])
  assert.equal(requestKind(u,m,t),k);
 for(const [u,m,t]of [['https://example.invalid/auth/login','POST','fetch'],[ME,'POST','fetch'],[BACK+'/auth/2fa/verify','POST','fetch'],
  [BACK+'/api/timesheets/example/authorise','POST','fetch'],[BACK+'/auth/forgot','POST','fetch'],[BACK+'/auth/reset','POST','fetch'],
  [ME+'?other=1','GET','fetch'],[BACK+'/auth/login','GET','document'],['http://testmode.arthur-rai.co.uk/','GET','document'],
  ['https://test-cloudtms-backend.kier-88a.workers.dev.evil.invalid/auth/login','POST','fetch'],[ME+'?other=1','OPTIONS','fetch']]){
  const n=networkGuard();assert.equal(n.accept(u,m,t),false);
 }
 assert.equal(requestKind(ME,'OPTIONS','fetch'),'PREFLIGHT');
});
test('login and ordinary refresh have hard one-request limits; forbidden writes cannot export state',()=>{
 for(const path of ['/auth/login','/auth/refresh']){
  const n=networkGuard();assert.equal(n.accept(BACK+path,'POST','fetch'),true);assert.equal(n.accept(BACK+path,'POST','fetch'),false);
  assert.throws(()=>n.assertAdmission());
 }
 const n=networkGuard();n.accept(BACK+'/auth/login','POST','fetch');n.assertAdmission();
 n.accept(BACK+'/api/users/me/grid-prefs','POST','fetch');assert.throws(()=>n.assertAdmission());
 assert.deepEqual(Object.keys(n.summary()).sort(),['blockedReadRequests','forbiddenWriteRequests','loginRequests','refusedRequests','refreshRequests'].sort());
});

test('actual transport never follows redirects or fulfils rejected/failed requests',async()=>{
 for(const scenario of ['ok','redirect','failed','forbidden']){
  const n=networkGuard(),calls=[];const url=scenario==='forbidden'?BACK+'/api/write':BACK+'/auth/login';
  const response={status:()=>scenario==='redirect'?307:200,dispose:async()=>calls.push('dispose')};
  const route={request:()=>({url:()=>url,method:()=> 'POST',resourceType:()=> 'fetch'}),
   abort:async()=>calls.push('abort'),fulfill:async options=>{assert.equal(options.response,response);calls.push('fulfill');},
   fetch:async options=>{assert.deepEqual(options,{maxRedirects:0,timeout:15000});calls.push('fetch');if(scenario==='failed')throw Error('SYNTHETIC_ONLY');return response;}};
  await guardedRoute(route,n);
  if(scenario==='ok'){assert.deepEqual(calls,['fetch','fulfill','dispose']);n.assertAdmission();}
  else{assert.equal(calls.includes('fulfill'),false);assert.equal(calls.includes('abort'),true);assert.throws(()=>n.assertAdmission());}
  if(scenario==='forbidden')assert.equal(calls.includes('fetch'),false);
 }
});
test('owned paths refuse shared state, parent traversal, C development and unowned locations',()=>{
 assert.equal(ownedDirectory(out),out);
 for(const p of [ROOT+'/.codex-tmp',out+'/child',ROOT+'/test-auth-refresh-20261017-'+'a'.repeat(32),
  'C:/tmp/test-auth-refresh-20261017-'+'a'.repeat(32),'D:/Development/GitHub/TEST-Frontend/tests/e2e/.auth/user.json'])assert.throws(()=>ownedDirectory(p));
});
test('child environment never inherits credentials, Node options, debug hooks or original TEMP',()=>{
 const env=childEnvironment(out);
 for(const key of ['E2E_USER_EMAIL','E2E_USER_PASSWORD','NODE_OPTIONS','NODE_V8_COVERAGE','PW_TEST_HTML_REPORT','CI'])assert.equal(Object.hasOwn(env,key),false);
 assert.equal(env.TEMP,path.join(out,'tmp'));assert.equal(env.PWDEBUG,'0');assert.equal(env.DEBUG,'');
});
test('root and worker entry points refuse before source/env access without authority',async()=>{
 await assert.rejects(()=>runAuthRefresh([]),/AUTH_ROOT_OUTSIDE_NATIVE_AUTHORITY_REQUIRED/);
 await assert.rejects(()=>runWorker(),/AUTH_WORKER_ONLY/);
});
test('closed results reject extra fields, unsafe codes, nonboolean closure and malformed counters',()=>{
 const good={version:'TEST_AUTH_REFRESH_PRIVATE_RESULT_V2',status:'HOLD',code:'AUTH_2FA_REQUIRED',browserClosed:true,
  loginRequests:1,refreshRequests:0,blockedReadRequests:0,forbiddenWriteRequests:0,refusedRequests:0,diagnostic:{phase:'BEFORE_EXPLICIT_ADMISSION',admission:null,blockedWriteLabel:'NONE',blockedWriteMethod:'NONE'}};
 assert.equal(closedResult(good).code,'AUTH_2FA_REQUIRED');
 for(const mutate of [v=>v.email='synthetic@example.invalid',v=>v.code='raw error',v=>v.browserClosed='true',
  v=>v.refreshRequests=-1,v=>v.refreshRequests=null,v=>delete v.version]){
  const v=structuredClone(good);mutate(v);assert.throws(()=>closedResult(v));
 }
});

async function harness(mode={}){
 const calls=[];let writes=0;
 const context=vm.createContext({process:{env:{HEALTH_TEST_AUTH_OWNED_OUT:out}}});
 const stubs={
  'node:fs':{mkdirSync:()=>{},default:{existsSync:()=>!!mode.exists}},
  'node:path':{dirname:path.dirname,default:path},
  './policy.mjs':{ownedDirectory,check},
  './credentials-child.mjs':{credentials:()=>{calls.push('credentials');return {email:'synthetic@example.invalid',password:'SYNTHETIC_ONLY'};}},
  './admission.mjs':{admission:async()=>({meStatus:200,actorUuid:true,admin:true,workspaceStatus:200,contract:true,queries:true,...mode.admission})},
  'file:///D:/Development/GitHub/TEST-Frontend/node_modules/@playwright/test/index.mjs':{
   expect:()=>({toBeHidden:async()=>{if(mode.overlayVisible)throw Error('SYNTHETIC_UI_HELD');}})}
 };
 const module=new vm.SourceTextModule(fs.readFileSync(path.join(here,'forward/authenticate.mjs'),'utf8'),{context});
 await module.link(async id=>{const values=stubs[id];assert.ok(values,id);
  return new vm.SyntheticModule(Object.keys(values),function(){for(const[k,v]of Object.entries(values))this.setExport(k,v);},{context});});
 await module.evaluate();
 const locator=()=>({count:async()=>1,isVisible:async()=>true,fill:async()=>{calls.push('fill');},click:async()=>{calls.push('click');}});
 const page={goto:async()=>{calls.push('goto');},getByTestId:locator,
  locator:selector=>selector==='#tfaOverlay'?{isVisible:async()=>!!mode.tfa}:locator(),
  waitForFunction:async()=>{},context:()=>({storageState:async()=>{calls.push('state');writes++;}})};
 const network={recordAdmission:()=>{},assertAdmission:()=>{if(mode.network)throw Error('AUTH_NETWORK_BOUNDARY_HELD');}};
 let failure;
 try{await module.namespace.authenticate({page,network});}catch(e){failure=e.message;}
 return {calls,writes,failure};
}
test('actual transformed approved UI callback saves once only after genuine-shaped admission',async()=>{
 const r=await harness();assert.equal(r.writes,1);assert.deepEqual(r.calls,['credentials','goto','fill','fill','click','state']);
});
test('2FA, hidden UI failure, failed admission, forbidden network and existing state never save',async()=>{
 for(const mode of [{tfa:true},{overlayVisible:true},{admission:{meStatus:401}},{admission:{actorUuid:false}},
  {admission:{admin:false}},{admission:{workspaceStatus:401}},{admission:{contract:false}},{admission:{queries:false}},
  {network:true},{exists:true}]){const r=await harness(mode);assert.equal(r.writes,0);assert.ok(r.failure);}
 assert.equal((await harness({tfa:true})).failure,'AUTH_2FA_REQUIRED');
});
test('actual page admission returns only booleans/statuses via fixed authFetch with refresh suppression',async()=>{
 const requests=[];globalThis.authFetch=async(url,options)=>{requests.push([url,options]);return {status:200,json:async()=>url===ME?
  {user:{id:'451d54a1-906a-4234-b535-d4a5977f9c97',roles:['admin'],email:'synthetic@example.invalid',email_signature_html:'synthetic'}}:
  {contract:'WEEKLY_SOURCE_IMPORT_WORKSPACE_V1',queries:{rows:[]}}};};
 try{
  const value=await admission({evaluate:async fn=>fn()});
  assert.deepEqual(value,{meStatus:200,actorUuid:true,admin:true,workspaceStatus:200,contract:true,queries:true});
  assert.deepEqual(requests.map(r=>r[0]),[ME,WORKSPACE]);
  assert.ok(requests.every(r=>r[1].auth401Mode==='return'&&r[1].signal instanceof AbortSignal));
  assert.doesNotMatch(JSON.stringify(value),/451d54a1|synthetic|email|signature|token/);
 }finally{delete globalThis.authFetch;}
});
test('actual child loader reads only through its worker API and restores every environment key',async()=>{
 let loaded;
 const fake={send:()=>{},argv:['node','worker','--owned-auth-worker'],env:{SAFE:'unchanged'},
  loadEnvFile:file=>{loaded=file;Object.assign(fake.env,{E2E_USER_EMAIL:'synthetic@example.invalid',E2E_USER_PASSWORD:'SYNTHETIC_ONLY',NODE_OPTIONS:'SYNTHETIC_CANARY'});}};
 const context=vm.createContext({process:fake});
 const module=new vm.SourceTextModule(fs.readFileSync(path.join(here,'credentials-child.mjs'),'utf8'),{context});
 await module.link(()=>new vm.SyntheticModule(['ENV_FILE','check'],function(){this.setExport('ENV_FILE','APPROVED_ENV_PATH_TOKEN');this.setExport('check',check);},{context}));
 await module.evaluate();const value=module.namespace.credentials();
 assert.equal(loaded,'APPROVED_ENV_PATH_TOKEN');assert.equal(value.password,'SYNTHETIC_ONLY');assert.deepEqual(fake.env,{SAFE:'unchanged'});
 fake.send=undefined;assert.throws(()=>module.namespace.credentials(),/AUTH_WORKER_ONLY/);
 fake.send=()=>{};fake.loadEnvFile=()=>{fake.env.E2E_USER_PASSWORD='SYNTHETIC_PARTIAL';throw Error('SYNTHETIC_LOAD_FAILURE');};
 assert.throws(()=>module.namespace.credentials(),/SYNTHETIC_LOAD_FAILURE/);assert.deepEqual(fake.env,{SAFE:'unchanged'});
});
