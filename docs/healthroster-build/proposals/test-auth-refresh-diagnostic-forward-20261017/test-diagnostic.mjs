// Offline only; all users/URLs/payload values below are synthetic canaries.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import vm from 'node:vm';import {fileURLToPath} from 'node:url';
import {BACK,FRONT,ME,WORKSPACE,ROOT,networkGuard,closedDiagnostic,closedAdmission,ownedDirectory,check} from './policy.mjs';
import {networkGuard as oldGuard} from '../test-auth-refresh-ps5-forward-20261017/policy.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));

test('forbidden static routes expose only source-label/method enums; query and IDs never leave',()=>{
 for(const [suffix,method,label,verb]of [['/api/changes/ping?private=SYNTHETIC_QUERY','POST','STATIC_CHANGES_PING','POST'],
  ['/api/users/me/grid-prefs?private=SYNTHETIC_QUERY','PATCH','STATIC_USER_GRID_PREFS','PATCH'],
  ['/api/00000000-0000-0000-0000-000000000999/secret','DELETE','OTHER_NORMAL_BACKEND','DELETE'],
  ['/api/changes/ping','CUSTOM_SYNTHETIC','STATIC_CHANGES_PING','OTHER']]){
  const n=networkGuard();assert.equal(n.accept(BACK+suffix,method,'fetch'),false);
  assert.deepEqual(n.diagnostics(),{phase:'BEFORE_EXPLICIT_ADMISSION',admission:null,blockedWriteLabel:label,blockedWriteMethod:verb});
  assert.doesNotMatch(JSON.stringify(n.diagnostics()),/SYNTHETIC|00000000|private|https|\/api\//);
  assert.throws(()=>n.assertAdmission(),/AUTH_NETWORK_BOUNDARY_HELD/);
 }
 const n=networkGuard();n.accept(BACK+'/api/changes/ping','POST','fetch');n.accept(BACK+'/api/users/me/grid-prefs','PATCH','fetch');
 assert.equal(n.diagnostics().blockedWriteLabel,'MULTIPLE');assert.equal(n.diagnostics().blockedWriteMethod,'MULTIPLE');
});

test('exact six actual admission fields are recorded with null before any returned observation',()=>{
 const n=networkGuard();assert.equal(n.diagnostics().admission,null);
 const observed={meStatus:401,actorUuid:false,admin:false,workspaceStatus:200,contract:true,queries:true};
 n.recordAdmission(observed);observed.meStatus=200;
 assert.deepEqual(n.diagnostics(),{phase:'EXPLICIT_ADMISSION_RETURNED',admission:{...observed,meStatus:401},blockedWriteLabel:'NONE',blockedWriteMethod:'NONE'});
 for(const value of [{...observed,email:'SYNTHETIC_PRIVATE'}, {...observed,actorUuid:'SYNTHETIC_UUID'}, {...observed,workspaceStatus:null}, {...observed,meStatus:600}])assert.throws(()=>closedAdmission(value));
 const good=n.diagnostics();for(const value of [{...good,url:'SYNTHETIC_PRIVATE'}, {...good,blockedWriteMethod:'GET'}, {...good,blockedWriteLabel:'SYNTHETIC_UUID'}, {...good,phase:'SUCCESS'}, {...good,blockedWriteLabel:'NONE',blockedWriteMethod:'POST'}])assert.throws(()=>closedDiagnostic(value));
});

test('all previous route acceptance, caps, violations and five counters remain unchanged',()=>{
 const old=oldGuard(),now=networkGuard();
 const routes=[[BACK+'/auth/login','POST','fetch'],[BACK+'/auth/refresh','POST','fetch'],[ME,'GET','fetch'],[WORKSPACE,'GET','fetch'],
  [FRONT+'/','GET','document'],[FRONT+'/js/main.js?v=1','GET','script'],[BACK+'/api/changes/ping','POST','fetch'],
  [BACK+'/api/users/me/grid-prefs','PATCH','fetch'],[BACK+'/api/unapproved','PUT','fetch'],['https://other.invalid/','GET','document']];
 for(let round=0;round<55;round++)for(const args of routes)assert.equal(now.accept(...args),old.accept(...args));
 assert.deepEqual(now.summary(),old.summary());assert.throws(()=>now.assertAdmission());assert.throws(()=>old.assertAdmission());
});

test('actual callback records observed admission on HOLD without allowing state export',async()=>{
 for(const scenario of ['blockedWrite','me401','workspace401']){
  const network=networkGuard();network.accept(BACK+'/auth/login','POST','fetch');
  if(scenario==='blockedWrite')network.accept(BACK+'/api/changes/ping','POST','fetch');
  const observed={meStatus:scenario==='me401'?401:200,actorUuid:true,admin:true,workspaceStatus:scenario==='workspace401'?401:200,contract:true,queries:true};
  let writes=0;const context=vm.createContext({process:{env:{HEALTH_TEST_AUTH_OWNED_OUT:ROOT+'/.codex-tmp/test-auth-refresh-20261017-'+'a'.repeat(32)}}});
  const stubs={'node:fs':{mkdirSync:()=>{},default:{existsSync:()=>false}},'node:path':{dirname:path.dirname,default:path},
   './policy.mjs':{ownedDirectory,check},'./credentials-child.mjs':{credentials:()=>({email:'SYNTHETIC_ONLY',password:'SYNTHETIC_ONLY'})},
   './admission.mjs':{admission:async()=>observed},'file:///D:/Development/GitHub/TEST-Frontend/node_modules/@playwright/test/index.mjs':{expect:()=>({toBeHidden:async()=>{}})}};
  const m=new vm.SourceTextModule(fs.readFileSync(path.join(here,'forward/authenticate.mjs'),'utf8'),{context});
  await m.link(async id=>new vm.SyntheticModule(Object.keys(stubs[id]),function(){for(const[k,v]of Object.entries(stubs[id]))this.setExport(k,v);},{context}));await m.evaluate();
  const locator={count:async()=>1,isVisible:async()=>true,fill:async()=>{},click:async()=>{}};
  const page={goto:async()=>{},getByTestId:()=>locator,locator:s=>s==='#tfaOverlay'?{isVisible:async()=>false}:locator,waitForFunction:async()=>{},context:()=>({storageState:async()=>writes++})};
  await assert.rejects(()=>m.namespace.authenticate({page,network}));assert.equal(writes,0);
  assert.deepEqual(network.diagnostics().admission,observed);assert.equal(network.diagnostics().phase,'EXPLICIT_ADMISSION_RETURNED');
 }
});
