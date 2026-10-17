// Only injected in-memory callbacks. Never connects tools, files, child processes or native.
import fs from 'node:fs';import assert from 'node:assert/strict';import test from 'node:test';import {SourceTextModule} from 'node:vm';
import {createHash} from 'node:crypto';import {pathToFileURL} from 'node:url';
import {buildReadInput} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/native-read.mjs';
const body=fs.readFileSync(new URL('./reference/e18-tools.js.txt',import.meta.url),'utf8'),AF=Object.getPrototypeOf(async function(){}).constructor;
new AF('tools','store','load','text','notify',body);
async function simulate(stage,scenario={}){
 const k='a'.repeat(40),owned='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/.codex-tmp/daily-genuine-stages-20261017-'+'1'.repeat(32),folder='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/.codex-tmp/daily-genuine-progress-20261017-'+'2'.repeat(32),state=new Map([['dailySelectedStage',stage],['dailyR10FinalK4',k],['dailyS0R8Actual',{sourceCommit:k,summary:{status:'S0_NATIVE_QUALIFIED_OWN_TURN_RELEASED'},exitCode:0,out:owned}],['dailyS1AdapterCommit','b'.repeat(40)],['dailyFourStageAdapterCommit','c'.repeat(40)]]),events=[],printed=[],closed='CLOSED_qualify_'+stage+'_'+'3'.repeat(16)+'.json';
 const result=o=>({exit_code:0,output:typeof o==='string'?o:JSON.stringify(o)});
 const fake={async exec_command(a){const c=a.cmd;events.push({type:'command',cmd:c});
  if(c.startsWith('$folder='))return result({folder,physical:folder});
  if(c.startsWith('C:'))return{session_id:99,output:''};
  if(c.includes('/progress.mjs ')){const expected=c.split(' ')[4];events.push({type:'ready',expected});return result({status:'ACTUAL_READY_OBSERVED',ready:scenario.wrongProgress?'WRONG':expected,stage,operatorSourceCommit:k,ownedDirectory:owned});}
  if(c.startsWith("@'\n")){
   const inner=c.slice(3,c.lastIndexOf("\n'@"));new SourceTextModule(inner);events.push({type:'generatedModule',source:inner});
   if(inner.includes('CLOSED_qualify_')){
    const transformed=inner.replace('import fs from"node:fs";','');let output='';
    await new AF('fs','process',transformed)({readdirSync:()=>[closed],readFileSync:()=>JSON.stringify({status:'PASS',code:'FIXTURE_STAGE_NATIVE_QUALIFIED',phase:'qualify',stage,browserClosed:scenario.noBrowserClose!==true})},{stdout:{write:v=>output+=v}});
    return result(output);
   }
   return result({input:{synthetic:true},receipt:{}});
  }
  if(c.startsWith('$value='))return result('');
  if(c.includes('HR_ROOT_PHASE=')){const phase=c.match(/HR_ROOT_PHASE='([^']+)'/)[1];events.push({type:'bookkeep',phase});return result({status:'ROOT_GENUINE_'+stage+'_'+phase+'_COMMITTED',commit:'d'.repeat(40)});}
  if(c.includes('ADAPTER_COMMIT')){events.push({type:'effects'});return result({status:'ACTUAL_'+stage+'_EFFECT_RECORDS_COMMITTED_ORIGINAL_AFTER_STILL_REQUIRED',commit:'e'.repeat(40)});}
  throw Error('UNKNOWN_FAKE_COMMAND');
 },async write_stdin(a){events.push({type:'control',value:a.chars});
  return a.chars==='RELEASE\n'||a.chars==='ABORT\n'?{exit_code:scenario.badExit?1:0,output:''}:{session_id:99,output:''};
 },async mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback(){
  events.push({type:'fakeNative'});if(scenario.nativeThrow)throw Error('SYNTHETIC_NATIVE_THROW');
  return{structuredContent:{accepted:scenario.nativeHeld!==true,rolledBack:true,rollbackVerification:{connectedFresh:true},database:'cloudtms_test_clone'}};
 }};
 await new AF('tools','store','load','text','notify',body)(fake,(x,v)=>state.set(x,v),x=>state.get(x),x=>printed.push(x),()=>{});
 return {events,printed,state};
}
for(const stage of ['S1','S1a','S2','S3','S4'])test('actual full body with fake callbacks: '+stage,async()=>{
 const r=await simulate(stage),final=r.printed.at(-1);assert.equal(final.status,'ACTUAL_STAGE_ORIGINAL_PUBLISHED',JSON.stringify(final));
 assert.equal(r.events.filter(x=>x.type==='fakeNative').length,['S1','S2','S3'].includes(stage)?4:2);
 assert.deepEqual(r.events.filter(x=>x.type==='control').map(x=>x.value),['BEFORE_SAVED\n','SETUP_COMMITTED\n','AFTER_SAVED\n','RELEASE\n']);
 assert.deepEqual(r.events.filter(x=>x.type==='bookkeep').map(x=>x.phase),['BEFORE','SETUP','AFTER','PUBLISH']);
});
test('wrong READY refuses before native and aborts only own captured session',async()=>{const r=await simulate('S2',{wrongProgress:true});assert.equal(r.printed.at(-1).status,'ROOT_STAGE_ACTUAL_HOLD');assert.equal(r.events.filter(x=>x.type==='fakeNative').length,0);assert.deepEqual(r.events.filter(x=>x.type==='control').map(x=>x.value),['ABORT\n']);});
for(const scenario of [{nativeHeld:true},{nativeThrow:true}])test('native uncertainty stops and reconciles without retry '+JSON.stringify(scenario),async()=>{const r=await simulate('S2',scenario);assert.equal(r.printed.at(-1).status,'ROOT_STAGE_ACTUAL_HOLD');assert.equal(r.events.filter(x=>x.type==='fakeNative').length,1);assert.equal(r.events.filter(x=>x.type==='bookkeep').length,0);assert.deepEqual(r.events.filter(x=>x.type==='control').map(x=>x.value),['ABORT\n']);});
test('nonzero coordinator exit never publishes',async()=>{const r=await simulate('S4',{badExit:true});assert.equal(r.printed.at(-1).code,'ORIGINAL_STAGE_CLOSE_NOT_PROVED');assert.equal(r.events.filter(x=>x.type==='bookkeep'&&x.phase==='PUBLISH').length,0);});
test('actual qualifier browser-close false never publishes',async()=>{const r=await simulate('S4',{noBrowserClose:true});assert.equal(r.printed.at(-1).status,'ROOT_STAGE_ACTUAL_HOLD');assert.equal(r.events.filter(x=>x.type==='bookkeep'&&x.phase==='PUBLISH').length,0);});
test('actual generated packet code accepts genuine pure original/aux builder frames for every stage and phase without file access',async()=>{
 let originals=0,auxiliary=0;
 for(const stage of ['S1','S1a','S2','S3','S4']){
  const r=await simulate(stage);
  for(const event of r.events.filter(x=>x.type==='generatedModule'&&x.source.includes('ROOT_STAGE_NATIVE_INPUT_REFUSED'))){
   const inner=event.source,m=inner.match(/phase="(BEFORE|AFTER)",aux=(true|false)/);assert(m);const phase=m[1],aux=m[2]==='true';
   const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:12345,backendPid:23456,key:[20261013,1],turnId:'hr_turn_'+'a'.repeat(32),takenAt:'2026-10-17T10:00:00.000Z',task:'SYNTHETIC_SOURCE_PEER_ONLY'};
   const built=buildReadInput({stage,phase,identity,actorId:'11111111-1111-1111-1111-111111111111'}),bytes=Buffer.from(JSON.stringify(built)),writes=[];let output='';
   const fakeFs={realpathSync:{native:x=>x},lstatSync:()=>({isFile:()=>true,isSymbolicLink:()=>false,size:bytes.length}),readFileSync:()=>bytes,writeFileSync:(...a)=>writes.push(a)};
   const program=inner.replace('import fs from "node:fs";import{createHash}from"node:crypto";import{pathToFileURL}from"node:url";','');
   await new AF('fs','createHash','pathToFileURL','process',program)(fakeFs,createHash,pathToFileURL,{stdout:{write:v=>output+=v}});
   const v=JSON.parse(output);assert.equal(v.receipt.phase,phase);assert.equal(v.input.database,'agency_test');assert.equal(writes.length,aux?1:0);
   if(aux){auxiliary++;assert.equal(v.receipt.version,stage==='S1'?'S1_SCOPED_OBSERVER_INPUT_V1':'S23_SCOPED_OBSERVER_INPUT_V1');assert.equal(v.receipt.mode,'EFFECTS');assert.equal(Object.hasOwn(v.receipt,'stage'),stage!=='S1');assert.equal(Object.hasOwn(v.receipt,'statements'),false);}else{originals++;assert.deepEqual(v,built);}
  }
 }
 assert.equal(originals,10);assert.equal(auxiliary,6);
});

test('complete e18 pin layer reverses to executed R10 body',()=>{const p=JSON.parse(fs.readFileSync(new URL('./ROOT_HELPER_PIN_LAYER.json',import.meta.url))),old=fs.readFileSync(new URL('./reference/executed-r10-tools.js.txt',import.meta.url));assert.equal(body.split(p.after).length,2);assert(Buffer.from(body.replace(p.after,()=>p.before)).equals(old));assert.equal(createHash('sha256').update(Buffer.from(body)).digest('hex'),p.afterSha256);assert(body.includes("HR_ROOT_HELPER_COMMIT='e18d4b670d04538399aab94b9d3a4566e385a0e4'"));});
