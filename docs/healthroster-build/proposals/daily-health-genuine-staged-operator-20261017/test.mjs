// Entirely offline synthetic protocol/transport records. No auth state or browser is opened.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {spawn,execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
import {ROOT,STAGES,BACK,request,route,qualifyResponse,entryText,sha,PRIVATE_STATE} from './contract.mjs';
import {networkGuard,guardedRoute} from './network.mjs';
import {buildReadInput,readEnvelope,scopes,routineTuple,routineSelection} from './native-read.mjs';
import {assertBefore,assertAfter,acquireSession,persistAttempt,assertSetupEntry,assertEffectAdmission} from './stages.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const id='e7d84371-0000-0000-0000-000000000001'; // Synthetic format vector, no native row.
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:71,backendPid:701,turnId:'hr_turn_'+'e'.repeat(32),key:[20261013,1]};
const clone=v=>structuredClone(v);
const response=stage=>stage==='S1'?{ok:true,replay:false,client:{...request(stage),client_settings:undefined},client_settings:{id,...request(stage).client_settings,client_id:request(stage).id,effective_from:'1900-01-01'},policy:{healthroster:{},nhsp:{}}}
 :stage==='S1a'?{hospital:{...request(stage),client_id:request('S1').id}}
 :stage==='S2'?{candidate:{...request(stage),tms_ref:'SYNTHETIC_FORMAT_ONLY'},tsfin:{enqueued:0,ran_now:0,picked:0,ok:0,fail:0,auto_assigned_timesheets:0}}
 :stage==='S3'?{rate:{id,...request(stage),disabled_at_utc:null}}:{timesheet_id:id,current_timesheet_id:id,created_unprocessed:true};
const frame=(stage='S0',phase='BEFORE')=>({version:'DAILY_GENUINE_NATIVE_AGGREGATE_V1',stage,phase,database:'cloudtms_test_clone',databaseOid:71,backendPid:702,holderPid:701,holderGranted:true,readOnly:true,noRoleSwitch:true,observedAt:new Date().toISOString(),testIdentity:true,legacyOne:true,applyingCount:0,catalogueComplete:true,actorAllowed:true,
 scope:Object.fromEntries(scopes.map(k=>[k,{count:0,sha256:sha('synthetic-empty-'+k)}])),facts:{settings:false,clientMapping:false,candidate:false,rate:false,timesheet:false,financial:false,audit:true},ids:{settings:null,rates:null,timesheets:null,financials:null},catalogueSha256:sha('synthetic-catalogue')});
const statement=(number,v)=>({statement:number,command:'SELECT',rowCount:1,fields:['fixture_read'],rows:[{fixture_read:v}],rowsTruncated:false});
const envelope=(built,v)=>({database:'cloudtms_test_clone',databaseTarget:'agency_test',label:built.receipt.label,sqlSha256:built.receipt.sqlSha256,statementCount:2,accepted:true,rolledBack:true,
 statements:[{statement:1,command:'SET',rowCount:null,fields:[],rows:[],rowsTruncated:false},statement(2,clone(v))],
 rollbackVerification:{connectedFresh:true,database:'cloudtms_test_clone',statements:[statement(1,clone(v))]}});
test('original request bodies remain exact approved recipe; S1 native baseline1900 is not the request date',()=>{
 const r=JSON.parse(fs.readFileSync(here+'/reference/daily-health-genuine-fixture-recipe-20261017/RECIPE.json'));
 for(const s of ['S1','S2','S3','S4'])assert.deepEqual(request(s),r.stages.find(x=>x.id===s).body);
 assert.equal(request('S1').client_settings.effective_from,'2026-10-01');assert.equal(qualifyResponse('S1',200,response('S1')).baseline,'1900-01-01');
 const bad=response('S1');bad.client_settings.effective_from='2026-10-01';assert.throws(()=>qualifyResponse('S1',200,bad));
});
test('routine identity uses its captured schema consistently for bare/qualified names and never accepts a conflicting schema',()=>{
 assert.deepEqual(routineTuple({schema:'public',identity:'trg_tsfin_client_hospitals_wakeup()'}),{schema:'public',name:'trg_tsfin_client_hospitals_wakeup'});
 assert.deepEqual(routineTuple({schema:'public',identity:'public.trg_tsfin_client_hospitals_wakeup()'}),{schema:'public',name:'trg_tsfin_client_hospitals_wakeup'});
 assert.throws(()=>routineTuple({schema:'private',identity:'public.trg_tsfin_client_hospitals_wakeup()'}));
 assert(routineSelection.some(r=>r.schema==='private'&&r.name==='_candidate_feature_enabled_current_v1'));
 const built=buildReadInput({stage:'S0',phase:'BEFORE',identity,actorId:id});assert(!built.input.sql.includes(id));assert.equal(built.receipt.actorBindingSha256,sha(id));
});
test('strict actual responses reject nominal wrappers, replay, wrong IDs, nonempty drain and empty200',()=>{
 for(const s of ['S1','S1a','S2','S3','S4']){
  assert.ok(qualifyResponse(s,200,response(s)));assert.throws(()=>qualifyResponse(s,500,response(s)));assert.throws(()=>qualifyResponse(s,200,{ok:true,data:response(s)}));assert.throws(()=>qualifyResponse(s,200,{}));
 }
 let b=response('S1');b.replay=true;assert.throws(()=>qualifyResponse('S1',200,b));
 b=response('S2');b.tsfin.auto_assigned_timesheets=1;assert.throws(()=>qualifyResponse('S2',200,b));
 b=response('S3');b.rate.date_from='2026-09-01';assert.throws(()=>qualifyResponse('S3',200,b));
 b=response('S4');b.current_timesheet_id='e7d84371-0000-0000-0000-000000000002';assert.throws(()=>qualifyResponse('S4',200,b));
});
test('only armed exact one current stage POST/body can be forwarded; all other mutation paths remain aborted',()=>{
 for(const s of ['S1','S1a','S2','S3','S4']){
  for(const [url,method,body]of [[BACK+route[s],'POST',JSON.stringify(request(s))],[BACK+route[s]+'?retry=1','POST',JSON.stringify(request(s))],[BACK+route[s],'PATCH','{}'],[BACK+'/auth/refresh','POST','{}'],[BACK+'/auth/login','POST','{}'],[BACK+'/api/users/me/grid-prefs','PATCH','{}'],[BACK+'/api/banking/pay','POST','{}']]){
   const n=networkGuard(s);assert.equal(n.accept(url,method,'fetch',body),false);
  }
  let n=networkGuard(s);n.arm();assert.equal(n.accept(BACK+route[s],'POST','fetch',JSON.stringify(request(s))),true);assert.equal(n.accept(BACK+route[s],'POST','fetch',JSON.stringify(request(s))),false);assert.throws(()=>n.assertClean(1));assert.equal(n.summary().businessWrites,1);
  n=networkGuard(s);n.arm();assert.equal(n.accept(BACK+route[s],'POST','fetch','{}'),false);assert.equal(n.summary().businessWrites,0);
 }
});
test('one exact startup ping remains aborted; query/method/origin variations and second ping are fatal',()=>{
 const n=networkGuard('S0');assert.equal(n.accept(BACK+'/api/changes/ping','POST','fetch'),false);n.assertClean(0);assert.equal(n.summary().suppressedStartupRequests,1);
 assert.equal(n.accept(BACK+'/api/changes/ping','POST','fetch'),false);assert.throws(()=>n.assertClean(0));
 for(const [url,method]of [[BACK+'/api/changes/ping?q=x','POST'],[BACK+'/api/changes/ping','PATCH'],['https://invalid.example/api/changes/ping','POST']]){const x=networkGuard('S0');assert.equal(x.accept(url,method,'fetch'),false);assert.throws(()=>x.assertClean(0));}
});
test('guarded transport aborts redirects without forwarding to redirected origin or fabricating success',async()=>{
 const n=networkGuard('S1');n.arm();let fetches=0,aborts=0,fulfilled=0;
 await guardedRoute({request:()=>({url:()=>BACK+route.S1,method:()=>'POST',resourceType:()=>'fetch',postData:()=>JSON.stringify(request('S1'))}),
  fetch:async options=>{fetches++;assert.equal(options.maxRedirects,0);return{status:()=>302,dispose:async()=>{}};},abort:async()=>{aborts++;},fulfill:async()=>{fulfilled++;}},n);
 assert.equal(fetches,1);assert.equal(aborts,1);assert.equal(fulfilled,0);assert.throws(()=>n.assertClean(1));
});
test('native envelopes require exact SQL, main/fresh actual one-row frames, role/holder/clock/scope consistency',()=>{
 const b=buildReadInput({stage:'S0',phase:'BEFORE',identity,actorId:id}),v=frame();assert.ok(readEnvelope(envelope(b,v),b.receipt));
 const mutations=[r=>r.sqlSha256='0'.repeat(64),r=>r.accepted=false,r=>r.rolledBack=false,r=>r.database='uofvkfi5',r=>r.statements[1].rowsTruncated=true,r=>r.statements[1].rowCount='1',r=>r.rollbackVerification.connectedFresh=false,
  r=>r.rollbackVerification.statements[0].rows[0].fixture_read.scope.outbox.count=1,r=>r.statements[1].rows[0].fixture_read.holderGranted=false,
  r=>r.statements[1].rows[0].fixture_read.readOnly=false,r=>r.statements[1].rows[0].fixture_read.actorAllowed=false,r=>r.statements[1].rows[0].fixture_read.applyingCount=1,
  r=>r.statements[1].rows[0].fixture_read.observedAt='2000-01-01T00:00:00Z',r=>delete r.statements[1].rows[0].fixture_read.scope.overrides,
  r=>r.statements[1].rows[0].fixture_read.catalogueComplete=false,r=>r.statements[1].rows[0].fixture_read.privateRow='forbidden',r=>r.statements[1].rows[0].fixture_read.facts.extra=true,
  r=>r.statements[1].rows[0].fixture_read.ids.settings=['not-a-native-uuid']];
 for(const mutate of mutations){const r=envelope(b,v);mutate(r);assert.throws(()=>readEnvelope(r,b.receipt));}
 for(const bad of ["x';delete from clients;--",'E7D84371-0000-0000-0000-000000000001'])assert.throws(()=>buildReadInput({stage:'S0',phase:'BEFORE',identity,actorId:bad}));
});
test('S0/S1 complete vacancy and exact prefix/currentness reject collisions before request',()=>{
 const v=frame();assertBefore('S0',v,{qualified:[]});assertBefore('S1',v,{qualified:['S0']});
 for(const k of scopes){const changed=clone(v);changed.scope[k].count=1;assert.throws(()=>assertBefore('S0',changed,{qualified:[]}));}
 assert.throws(()=>assertBefore('S3',v,{qualified:['S0']}));
 const current=clone(v);current.scope.clients.count=current.scope.settings.count=current.scope.audit.count=1;current.facts.settings=true;
 assertBefore('S1a',current,{qualified:['S0','S1'],after:clone(current)});current.scope.settings.sha256=sha('changed');assert.throws(()=>assertBefore('S1a',current,{qualified:['S0','S1'],after:clone({...current,scope:{...current.scope,settings:{count:1,sha256:sha('old')}}})}));
});
test('genuine S1a is mandatory before S2/S4; duplicate/global mapping and wrong association refuse',()=>{
 const before=frame('S1a'),after=clone(before);after.scope.mappings.count=1;after.facts.clientMapping=true;
 assertAfter('S1a',before,after,{mappingId:request('S1a').id});
 for(const mutate of [r=>r.scope.mappings.count=2,r=>r.facts.clientMapping=false,r=>r.scope.outbox.count=1,r=>r.scope.timesheets.count=1]){const b=clone(after);mutate(b);assert.throws(()=>assertAfter('S1a',before,b,{}));}
 const s4=frame('S4');for(const k of ['clients','settings','mappings','candidates','rates','audit'])s4.scope[k].count=1;s4.facts.settings=s4.facts.candidate=s4.facts.rate=true;
 const prior={qualified:['S0','S1','S1a','S2','S3']};assert.throws(()=>assertBefore('S4',s4,prior),/MAPPING/);s4.facts.clientMapping=true;assertBefore('S4',s4,prior);
 const wrong=response('S1a');wrong.hospital.client_id=id;assert.throws(()=>qualifyResponse('S1a',200,wrong));
});
test('S4 financial/current/history/outbox are independent native predicates; HTTP three keys never suffice',()=>{
 const before=frame('S4'),after=clone(before);after.scope.timesheets.count=after.scope.financials.count=1;after.scope.audit.count=1;after.facts.timesheet=after.facts.financial=true;after.ids.timesheets=[id];after.ids.financials=[id];assertAfter('S4',before,after,{timesheetId:id});
 for(const mutate of [x=>x.scope.timesheets.count=2,x=>x.scope.financials.count=2,x=>x.scope.outbox.count=1,x=>x.facts.financial=false,x=>x.facts.timesheet=false,x=>x.scope.overrides.count=1,x=>x.catalogueSha256=sha('drift')]){const bad=clone(after);mutate(bad);assert.throws(()=>assertAfter('S4',before,bad,{timesheetId:id}));}
});
test('committed setup entry is exact and bound to native before/fresh source and effect evidence bytes',()=>{
 const b=buildReadInput({stage:'S1',phase:'BEFORE',identity,actorId:id}),bytes=Buffer.from('synthetic-native-envelope'),effect=Buffer.from('synthetic-root-effect');
 const e={sourceCommit:'e'.repeat(40),beforeSqlSha256:b.receipt.sqlSha256,verificationSqlSha256:b.receipt.verificationSqlSha256,beforeEnvelopeSha256:sha(bytes),effectAdmissionSha256:sha(effect)};
 const text=entryText('S1',e);assert.ok(assertSetupEntry('retained original'+text,e,'S1',bytes,b.receipt,effect));
 assert.throws(()=>assertSetupEntry(text+text,e,'S1',bytes,b.receipt,effect));assert.throws(()=>assertSetupEntry(text.replace('POST ','PATCH '),e,'S1',bytes,b.receipt,effect));assert.throws(()=>assertSetupEntry(text,e,'S1',Buffer.from('different'),b.receipt,effect));
});
test('private state is only the exact R4 path, no shared state/credential reader in parent or child source',()=>{
 assert.equal(PRIVATE_STATE,ROOT+'/.codex-tmp/test-auth-refresh-20261017-e853b2b5d51c43b6c21d6a2668d3f25d/storage-state.json');
 for(const file of ['run-staged.mjs','worker.mjs','source.mjs']){
  const s=fs.readFileSync(here+'/'+file,'utf8');assert.doesNotMatch(s,/loadEnvFile|playwright-test\.env|tests\/e2e\/\.auth\/user\.json|page\.screenshot|tracing\.start|recordVideo|console\.log\(.*actor/);
 }
 const worker=fs.readFileSync(here+'/worker.mjs','utf8');assert.match(worker,/storageState:PRIVATE_STATE/);assert.doesNotMatch(worker,/storageState\(\{|readFileSync\(PRIVATE_STATE|sha\(PRIVATE_STATE/);
 assert.match(worker,/before-post-forward/);assert.match(worker,/turn\.signal\.addEventListener/);assert.match(worker,/turn\.close\(\)/);
});
test('exact ACL forward only changes the new owned directory namespace, preserving the complete PS5 parent',()=>{
 const before=fs.readFileSync(here+'/reference/secure-owned-auth-parent.ps1'),after=fs.readFileSync(here+'/secure-owned.ps1');
 assert.equal(after.toString().split('^daily-genuine-stages-20261017-[a-f0-9]{32}').length,2);
 assert(before.equals(Buffer.from(after.toString().replace('^daily-genuine-stages-20261017-[a-f0-9]{32}','^test-auth-refresh-20261017-[a-f0-9]{32}'))));
});
test('two separate Node processes cannot replace a lost-reply attempt or send a second request',async()=>{
 const parent=path.resolve(here,'../../../..').replaceAll('\\','/')+'/.codex-tmp';fs.mkdirSync(parent,{recursive:true});
 const dir=fs.mkdtempSync(parent+'/daily-genuine-offline-');const actual=fs.realpathSync.native(dir).replaceAll('\\','/');assert.equal(actual,dir.replaceAll('\\','/'));
 const script=`import {acquireSession,persistAttempt} from ${JSON.stringify(pathToFileURL(here+'/stages.mjs').href)};let release;try{release=acquireSession(process.argv[1]);persistAttempt(process.argv[1],'S1',{syntheticLostReply:true});process.stdout.write('ONE_PERSISTED');}catch{process.stdout.write('HELD');}finally{release?.();}`;
 const run=()=>new Promise(resolve=>{const child=spawn(process.execPath,['--input-type=module','-e',script,dir],{stdio:['ignore','pipe','ignore'],windowsHide:true});let s='';child.stdout.on('data',b=>s+=b);child.on('close',()=>resolve(s));});
 try{const results=await Promise.all([run(),run()]);assert.deepEqual(results.sort(),['HELD','ONE_PERSISTED']);assert.throws(()=>persistAttempt(dir,'S2',{}),/UNRESOLVED/);assert.equal(JSON.parse(fs.readFileSync(dir+'/S1_ATTEMPT.json')).syntheticLostReply,true);}
 finally{assert(actual.startsWith(parent+'/daily-genuine-offline-')&&fs.realpathSync.native(dir).replaceAll('\\','/')===actual);for(const f of fs.readdirSync(dir))fs.unlinkSync(dir+'/'+f);fs.rmdirSync(dir);}
});
