import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
const folder='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/docs/healthroster-build/proposals/daily-health-s2-s3-scoped-effects-observer-20261017/';
const source=n=>fs.readFileSync(folder+'reference/worker/'+n+'.js.txt','utf8');
test('uncached actual loader enters two unqualified external calls before empty drain; cache skips loader work',async()=>{
 const events=[];
 const context=vm.createContext({__SETTINGS_DEFAULTS_CACHE:{value:null,ts:0},__SETTINGS_DEFAULTS_TTL_MS:30000,console:{log(){},warn(){},error(){}},
  sbFetch:async(_env,url)=>{assert(url.endsWith('/settings_defaults?select=*&id=eq.1&limit=1'));events.push('GET');return{rows:[{}]};},
  _safeJsonParseMaybe:(_v,fallback)=>fallback,_asBool:(_v,fallback)=>fallback,_asPosInt:(_v,fallback)=>fallback,
  normalizeBankingPayWorkbenchSourceBuildParallelism:()=>{events.push('EXTERNAL_CALL');return 1;},
  sbRpc:()=>{throw Error('unexpected RPC');},runTsfinWorkerOnce:()=>{throw Error('unexpected worker');}});
 vm.runInContext(source('loadSettingsDefaults')+'\n'+source('tsfinTargetedDrainNow'),context);
 const invoke=()=>vm.runInContext("tsfinTargetedDrainNow({SUPABASE_URL:'https://offline.invalid'},{timesheetIds:[]})",context);
 const r=await invoke();assert.equal(r.enqueued,0);assert.equal(r.ran,0);assert.equal(r.done,true);assert.deepEqual(events,['GET','EXTERNAL_CALL','EXTERNAL_CALL']);
 await invoke();assert.deepEqual(events,['GET','EXTERNAL_CALL','EXTERNAL_CALL']);
 // Pure stand-ins identify call reachability only, not any genuine helper effect.
});
const obs=await import('file:///'+folder+'observer.mjs');
const ctx={identity:{physicalDatabase:'cloudtms_test_clone',databaseOid:12,backendPid:34,key:[20261013,1],turnId:'hr_turn_'+'a'.repeat(32)},actorId:'00000000-0000-f000-0000-000000000000',phase:'BEFORE',stage:'S2'};
const now=Date.parse('2026-10-17T12:00:00Z');
function fixture(){const{receipt}=obs.build('EFFECTS',ctx);return{receipt,v:{version:'S23_SCOPED_EFFECT_OBSERVER_V1',stage:'S2',mode:'EFFECTS',phase:'BEFORE',turnBindingSha256:receipt.turnBindingSha256,database:'cloudtms_test_clone',databaseOid:12,backendPid:56,holderPid:34,holderGranted:true,readOnly:true,noRoleSwitch:true,observedAt:new Date(now).toISOString(),testIdentity:true,legacyOne:true,applyingCount:'0',actorAllowed:true,observation:{scope:Object.fromEntries(obs.SCOPES.map(k=>[k,{count:'9007199254740993',sha256:'a'.repeat(64)}])),sequenceState:Object.fromEntries(obs.SEQUENCES.map(k=>[k,{lastValue:'9223372036854775807',isCalled:true}])),counterSeq:'9007199254740993'}}};}
test('both counts/four sequence pairs and counter retain lossless int63 strings',()=>{const{receipt,v}=fixture();assert.equal(obs.validateFrame(v,receipt,{now}),v);for(const key of obs.SCOPES)for(const bad of ['01','-1','9223372036854775808',9007199254740992]){const x=structuredClone(v);x.observation.scope[key].count=bad;assert.throws(()=>obs.validateFrame(x,receipt,{now}));}for(const key of obs.SEQUENCES){const x=structuredClone(v);x.observation.sequenceState[key].lastValue=9007199254740992;assert.throws(()=>obs.validateFrame(x,receipt,{now}));}});
test('cross-stage/phase/mode/turn/physical/backend substitutions remain refused',()=>{for(const patch of [{stage:'S3'},{phase:'AFTER'},{mode:'METADATA'},{turnBindingSha256:'b'.repeat(64)},{databaseOid:13},{database:'uofvkfi5'},{backendPid:34},{actorAllowed:false},{noRoleSwitch:false}]){const{receipt,v}=fixture();Object.assign(v,patch);assert.throws(()=>obs.validateFrame(v,receipt,{now}));}});
test('fixed builder refuses arbitrary stage/query/key/identity before generation',()=>{for(const patch of [{stage:'S4'},{stage:'S2;DELETE'},{identity:{...ctx.identity,query:'select 1'}},{identity:{...ctx.identity,key:[20261013,2]}},{identity:{...ctx.identity,backendPid:9007199254740992}}])assert.throws(()=>obs.build('METADATA',{...ctx,...patch}));});
