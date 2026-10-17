// Complete actual perform()/journal bodies under an explicitly synthetic VM.
// Browser, filesystem, current actor and native envelopes below are finite test doubles.
// This never reads private state, opens an IPC pipe/browser, calls HTTP or proves native success.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import {fileURLToPath,pathToFileURL} from 'node:url';
import * as contract from './contract.mjs';import * as native from './native-read.mjs';import * as network from './network.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const worker=fs.readFileSync(here+'/worker.mjs','utf8'),start=worker.indexOf('export async function perform('),end=worker.indexOf('\nif(process.argv[1]');assert(start>=0&&end>start);
const actualPerform=worker.slice(start,end).replace(/^export /,'');new vm.Script('('+actualPerform+')');
const journal=fs.readFileSync(here+'/stages.mjs','utf8').replace(/^import .*;\r?\n/gm,'').replace(/^export /gm,'');
const ACTOR='e7d84371-0000-0000-0000-000000000001',SETTINGS='e7d84371-0000-0000-0000-000000000002';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:71,backendPid:701,turnId:'hr_turn_'+'e'.repeat(32),key:[20261013,1]};
function memoryFS(files,events){
 let fd=0;const norm=f=>f.replaceAll('\\','/'),handles=new Map();return {
  existsSync:f=>files.has(norm(f)),lstatSync:f=>{f=norm(f);assert(files.has(f));return {isFile:()=>true,isSymbolicLink:()=>false,size:files.get(f).length};},
  readFileSync:(f,encoding)=>{f=norm(f);assert(files.has(f),'missing synthetic file');return encoding?files.get(f).toString():Buffer.from(files.get(f));},
  openSync:(f,flags)=>{f=norm(f);assert.equal(flags,'wx');if(files.has(f))throw Error('EEXIST');files.set(f,Buffer.alloc(0));handles.set(++fd,f);events.push(['open',path.basename(f)]);return fd;},
  writeSync:(n,s)=>{const f=handles.get(n);assert(f);files.set(f,Buffer.concat([files.get(f),Buffer.from(s)]));},fsyncSync:()=>{},closeSync:n=>handles.delete(n),
  unlinkSync:f=>{f=norm(f);assert(files.has(f));files.delete(f);},renameSync:(a,b)=>{a=norm(a);b=norm(b);assert(files.has(a));files.set(b,files.get(a));files.delete(a);},
  writeFileSync:(f,b,opts)=>{f=norm(f);if(opts?.flag==='wx'&&files.has(f))throw Error('EEXIST');files.set(f,Buffer.from(b));},
 };
}
function syntheticFrame(){return {version:'DAILY_GENUINE_NATIVE_AGGREGATE_V1',stage:'S1',phase:'BEFORE',database:'cloudtms_test_clone',databaseOid:71,backendPid:702,holderPid:701,holderGranted:true,readOnly:true,noRoleSwitch:true,observedAt:new Date().toISOString(),testIdentity:true,legacyOne:true,applyingCount:0,catalogueComplete:true,actorAllowed:true,
 scope:Object.fromEntries(native.scopes.map(k=>[k,{count:0,sha256:contract.sha('empty-'+k)}])),facts:{settings:false,clientMapping:false,candidate:false,rate:false,timesheet:false,financial:false,audit:true},ids:{settings:null,rates:null,timesheets:null,financials:null},catalogueSha256:contract.sha('synthetic-catalogue')};}
function environment(mode){
 const out='D:/SYNTHETIC_MEMORY_ONLY',events=[],files=new Map(),put=(f,v)=>files.set(out+'/'+f,Buffer.from(JSON.stringify(v)));
 const built=native.buildReadInput({stage:'S1',phase:'BEFORE',identity,actorId:ACTOR}),frame=syntheticFrame();
 const select=(n)=>({statement:n,command:'SELECT',rowCount:1,fields:['fixture_read'],rows:[{fixture_read:structuredClone(frame)}],rowsTruncated:false});
 const envelope={database:'cloudtms_test_clone',databaseTarget:'agency_test',label:built.receipt.label,sqlSha256:built.receipt.sqlSha256,statementCount:2,accepted:true,rolledBack:true,
  statements:[{statement:1,command:'SET',rowCount:null,fields:[],rows:[],rowsTruncated:false},select(2)],rollbackVerification:{connectedFresh:true,database:'cloudtms_test_clone',statements:[select(1)]}};
 put('S1_BEFORE_INPUT.json',built);put('S1_BEFORE_NATIVE.json',envelope);put('PRIVATE_BINDING.json',{identity,actorId:ACTOR});put('QUALIFIED.json',{qualified:['S0'],after:frame,generated:{}});
 const effect={version:'DAILY_GENUINE_ROOT_EFFECT_ADMISSION_V1',stage:'S1',operatorSourceCommit:'e'.repeat(40),catalogueSha256:frame.catalogueSha256,workerSourceSha256:'a'.repeat(64),
  nativeOwnerEvidence:'docs/healthroster-build/SYNTHETIC_NATIVE.json',schemaEffectEvidence:'docs/healthroster-build/SYNTHETIC_SCHEMA.json',canonicalBeforeEvidence:'docs/healthroster-build/SYNTHETIC_BEFORE.json',reviewRecord:'docs/healthroster-build/SYNTHETIC_REVIEW.json'};
 const effectBytes=Buffer.from(JSON.stringify(effect));
 const entry={sourceCommit:'e'.repeat(40),beforeSqlSha256:built.receipt.sqlSha256,verificationSqlSha256:built.receipt.verificationSqlSha256,beforeEnvelopeSha256:contract.sha(files.get(out+'/S1_BEFORE_NATIVE.json')),effectAdmissionSha256:contract.sha(effectBytes)};
 put('S1_ENTRY_BINDING.json',{entry,effectFile:'docs/healthroster-build/SYNTHETIC_EFFECT.json'});
 let routeHandler,forwarded=0,contexts=0,browserCloses=0,messageHandler;
 const nativeResponse={ok:true,replay:false,client:{...contract.request('S1')},client_settings:{...contract.request('S1').client_settings,id:SETTINGS,client_id:contract.CLIENT,effective_from:'1900-01-01'},policy:{healthroster:{},nhsp:{}}};
 const deliver=async(url,method,body=null)=>{
  let response,aborted=false;
  const req={url:()=>url,method:()=>method,postData:()=>body,resourceType:()=>'fetch'};
  await routeHandler({request:()=>req,abort:async()=>{aborted=true;},fulfill:async x=>{response=x.response;},fetch:async options=>{
   assert.equal(options.maxRedirects,0);
   if(method==='POST'){forwarded++;events.push(['forwarded']);if(mode==='lost-committed-reply')throw Error('SYNTHETIC_LOST_REPLY');}
   const status=method==='POST'&&mode==='partial-500'?500:200;
   const json=method==='POST'?nativeResponse:url.endsWith('/api/me')?{user:{id:mode==='wrong-actor'?'e7d84371-0000-0000-0000-000000000003':ACTOR,roles:['admin']}}:{contract:'WEEKLY_SOURCE_IMPORT_WORKSPACE_V1',queries:{rows:[]}};
   return {status:()=>status,json:async()=>json,dispose:async()=>{}};
  }});
  if(aborted||!response)throw Error('SYNTHETIC_ABORTED_FETCH');return response;
 };
 const page={setDefaultTimeout(){},setDefaultNavigationTimeout(){},goto:async()=>{},waitForFunction:async()=>{},
  evaluate:async(fn,args)=>{
   // Execute the exact supplied page callback, with the real closed route policy around its fake fetch.
   return new vm.Script('('+fn.toString()+')').runInNewContext({authFetch:async(url,init={})=>{
    const r=await deliver(url,init.method??'GET',init.body??null);return {status:r.status(),json:r.json};
   },AbortSignal,console:{log(){}}})(args);
  }};
 const browserFactory={chromium:{executablePath:()=> 'D:/Development/Caches/ms-playwright/SYNTHETIC_BROWSER.exe',launch:async()=>({
  newContext:async options=>{contexts++;assert.equal(options.storageState,contract.PRIVATE_STATE);assert.equal(options.serviceWorkers,'block');return {route:async(_glob,fn)=>{routeHandler=fn;},routeWebSocket:async()=>{},newPage:async()=>page,pages:()=>[page]};},
  close:async()=>{browserCloses++;if(mode==='late-close-abort'&&browserCloses===1)messageHandler?.('ABORT');if(mode==='late-unrelated-write')await deliver(contract.BACK+'/api/users/me/grid-prefs','PATCH','{}').catch(()=>{});}
 })}};
 const fakefs=memoryFS(files,events),fakeProcess={env:{HEALTH_GENUINE_OWNED_OUT:out},cwd:()=>out,send(){},on(n,fn){if(n==='message')messageHandler=fn;},removeListener(n,fn){if(n==='message'&&messageHandler===fn)messageHandler=null;}};
 const context=vm.createContext({...contract,...native,...network,fs:fakefs,path,pathToFileURL,process:fakeProcess,Buffer,Date,AbortController,AbortSignal,setTimeout,clearTimeout,
  here,sourcePreflight:()=>({}),assertReviewedExecutionSource:c=>{assert.equal(c,'e'.repeat(40));events.push(['reviewedSourceImage']);},ownedDirectory:p=>{assert.equal(p,out);return p;},physical:p=>p,read:f=>fakefs.readFileSync(f),
  committedRootFile:f=>f.endsWith('TEST_SETUP_MANIFEST.md')?Buffer.from(contract.entryText('S1',entry)):effectBytes,
  git:()=>Buffer.from('e'.repeat(40)),console:{log(){},error(){}}});
 new vm.Script(journal+'\nglobalThis.journalFunctions={assertBefore,assertAfter,assertSetupEntry,assertEffectAdmission,acquireSession,readJournal,persistAttempt,writeClosed,saveQualification};').runInContext(context);
 Object.assign(context,context.journalFunctions);
 return {out,files,events,browserFactory,context,mode,abort:()=>messageHandler?.('ABORT'),get counts(){return {forwarded,contexts,browserCloses};}};
}
// Node requires a real module namespace for vm.Script dynamic-import callbacks.
// The callback below returns a test-only namespace whose getter obtains the current synthetic control.
async function run(env,suffix,phase='submit'){
 const compiled=new vm.Script('('+actualPerform+')',{importModuleDynamically:async specifier=>{
  assert.equal(specifier,pathToFileURL(contract.ROOT+'/scripts/healthroster-turn-control.mjs').href);
  // The original source is retained; this narrow import substitute is explicitly synthetic, never a production seam.
  return await import('./worker-offline-turn.mjs');
 }});
 const options={'--phase':phase,'--stage':'S1','--effect-file':'docs/healthroster-build/SYNTHETIC_AFTER.json',resultName:'CLOSED_'+phase+'_S1_'+suffix.repeat(16)+'.json'};
 const turn={identity,signal:new AbortController().signal,assertHeld:async phase=>{env.events.push(['held',phase]);if(env.mode==='turn-loss'&&phase.endsWith('before-attempt'))throw Error('SHARED_TURN_LOST');},close:()=>env.events.push(['turnClosed'])};
 const module=await import('./worker-offline-turn.mjs');module.setSyntheticTurn(turn);
 return compiled.runInContext(env.context)(options,{browserFactory:env.browserFactory});
}
test('actual full worker persists before one forwarded POST and requires native AFTER rather than scoring success',async()=>{
 const e=environment('good'),r=await run(e,'a');assert.equal(r.status,'PASS',JSON.stringify({result:r,events:e.events}));assert.equal(r.code,'FIXTURE_AWAITING_NATIVE_AFTER');assert.equal(e.counts.forwarded,1);
 assert(e.files.has(e.out+'/S1_ATTEMPT.json'));assert(e.files.has(e.out+'/S1_AFTER_INPUT.json'));assert(!e.files.has(e.out+'/S1_QUALIFIED.json'));
 assert(e.events.findIndex(x=>x[0]==='open'&&x[1]==='S1_ATTEMPT.json')<e.events.findIndex(x=>x[0]==='forwarded'));
 assert.equal(e.events.filter(x=>x[0]==='turnClosed').length,1);
});
test('actual full worker retains lost committed reply across reopened controller; second send refuses',async()=>{
 const e=environment('lost-committed-reply');const first=await run(e,'b');assert.equal(first.status,'HOLD');assert(e.files.has(e.out+'/S1_RECONCILE_INPUT.json'));
 const attempt=Buffer.from(e.files.get(e.out+'/S1_ATTEMPT.json'));const second=await run(e,'c');assert.equal(second.status,'HOLD');assert.equal(e.counts.forwarded,1);assert(attempt.equals(e.files.get(e.out+'/S1_ATTEMPT.json')));assert(!e.files.has(e.out+'/S1_QUALIFIED.json'));
});
test('actual full worker partial500, changed current actor, turn loss and late forbidden write all remain held',async()=>{
 for(const mode of ['partial-500','wrong-actor','turn-loss','late-unrelated-write','late-close-abort']){
  const e=environment(mode),r=await run(e,'d');assert.equal(r.status,'HOLD',mode);assert.equal(e.counts.forwarded,['wrong-actor','turn-loss'].includes(mode)?0:1);assert(!e.files.has(e.out+'/S1_QUALIFIED.json'));
  if(!['wrong-actor','turn-loss'].includes(mode))assert(e.files.has(e.out+'/S1_ATTEMPT.json'));
 }
});
test('actual non-browser qualification aborted after all native/effect reads cannot advance the durable prefix',async()=>{
 const e=environment('good');assert.equal((await run(e,'e')).status,'PASS');
 const read=n=>JSON.parse(e.files.get(e.out+'/'+n));const built=read('S1_AFTER_INPUT.json'),before=read('S1_BEFORE_NATIVE.json');
 const after=structuredClone(before);after.label=built.receipt.label;after.sqlSha256=built.receipt.sqlSha256;
 for(const statement of [after.statements[1],after.rollbackVerification.statements[0]]){
  const v=statement.rows[0].fixture_read;v.phase='AFTER';v.observedAt=new Date().toISOString();
  for(const k of ['clients','settings','audit'])v.scope[k]={count:1,sha256:contract.sha('synthetic-created-'+k)};
  v.facts.settings=true;v.ids.settings=[SETTINGS];
 }
 const afterBytes=Buffer.from(JSON.stringify(after));e.files.set(e.out+'/S1_AFTER_NATIVE.json',afterBytes);
 const response=read('S1_RESPONSE.json');
 const effect={version:'DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1',stage:'S1',beforeEnvelopeSha256:contract.sha(e.files.get(e.out+'/S1_BEFORE_NATIVE.json')),afterEnvelopeSha256:contract.sha(afterBytes),responseProjectionSha256:contract.sha(JSON.stringify(response.projection)),
  canonicalAfterEvidence:'docs/healthroster-build/SYNTHETIC_A.json',exactApprovedChangesEvidence:'docs/healthroster-build/SYNTHETIC_B.json',backgroundAttributionEvidence:'docs/healthroster-build/SYNTHETIC_C.json'};
 e.context.committedRootFile=f=>{if(f.endsWith('SYNTHETIC_C.json'))e.abort();return Buffer.from(JSON.stringify(effect));};
 const journalBefore=Buffer.from(e.files.get(e.out+'/QUALIFIED.json')),r=await run(e,'f','qualify');
 assert.equal(r.status,'HOLD');assert.equal(r.code,'FIXTURE_BUDGET_HELD');assert(!e.files.has(e.out+'/S1_QUALIFIED.json'));assert(journalBefore.equals(e.files.get(e.out+'/QUALIFIED.json')));
});
