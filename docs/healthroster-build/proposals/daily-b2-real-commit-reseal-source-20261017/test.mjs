// Synthetic pure source tests; never actual admission, runtime, private state or Git.
import assert from 'node:assert/strict';import test from 'node:test';import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
import {core,st,b2,fixtureS0} from '../daily-genuine-b2-complete-inventory-forward-20261017/test-support.mjs';
import {exactPacketPredicate,assertPublicSource} from './admit-public.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),P='docs/healthroster-build/proposals/',COM=P+'daily-five-stage-producer-hash-forward-20261017/',B2=path.resolve(here,'../daily-genuine-b2-complete-inventory-forward-20261017');
const source=fs.readFileSync(here+'/payload/root/'+COM+'compose.mjs','utf8'),old=fs.readFileSync(here+'/parents/root/'+COM+'compose.mjs','utf8');
function selected(text){const context=vm.createContext({Buffer,TextDecoder,Uint8Array,createHash:crypto.createHash,...core,...st,...b2});new vm.Script(text.replace(/^import [^\n]*\n/gm,'').replace(/^export /gm,'')+'\nglobalThis.selected={composeStage,composeAll,names};').runInContext(context);return context.selected;}
const successor=selected(source),legacy=selected(old);
// Replay all22 original producer/source/evidence tests, changing only expected
// business plan key count12→14; the captured original file remains raw unchanged.
const originalTests=fs.readFileSync(here+'/parents/root/'+COM+'test.mjs','utf8');
const tests=originalTests.replace(/^import[^\n]*\n/,'').replace('plans and5 original effects,12/9 fields','plans and5 original effects,14/9 fields').replace('length,12)','length,14)');
const ctx=vm.createContext({assert,test,createHash:crypto.createHash,Buffer,...successor});new vm.Script(tests+'\nglobalThis.syntheticFixture=fixture;').runInContext(ctx);
test('business V2 composition uses exact closed CURRENT references and fresh B2 namespace',()=>{
 const f=ctx.syntheticFixture(),v=successor.composeAll(f.args);for(const r of v.records.filter((_,i)=>i%2===0)){const p=JSON.parse(r.bytes);assert.equal(p.version,'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V2');assert.equal(p.currentReadEvidence,b2.currentFiles(p.stage).evidence);assert.equal(p.currentConsumptionEvidence,b2.currentFiles(p.stage).consumptionEvidence);assert(r.file.includes('_B2_PLAN_'));assert(!JSON.stringify(p).includes('_R10_'));assert.equal(p.afterEffectDecision,null);assert.doesNotThrow(()=>core.exactPlan(p,p.stage,p.operatorSourceCommit));for(const key of['currentReadEvidence','currentConsumptionEvidence']){const x=structuredClone(p);delete x[key];assert.throws(()=>core.exactPlan(x,p.stage,p.operatorSourceCommit));}}
});
test('obsolete V1 composer refuses under new core; no nominal business plan can be emitted',()=>{const f=ctx.syntheticFixture();assert.throws(()=>legacy.composeStage({...f.args,stage:'S1'}));});
test('original S0 V1 plan remains valid without CURRENT; business-only composer cannot manufacture S0',()=>{const f=fixtureS0();assert.doesNotThrow(()=>core.exactPlan(f.plan,'S0',f.plan.operatorSourceCommit));assert.equal(f.plan.version,'DAILY_GENUINE_ROOT_STAGE_BOOKKEEPER_PLAN_V1');assert.equal(f.plan.currentReadEvidence,undefined);assert.throws(()=>successor.composeStage({...ctx.syntheticFixture().args,stage:'S0'}));});
const owners=[P+'daily-s1-after-clock-adapter-20261017/',P+'daily-original-stages-root-effect-adapter-20261017/'];
function packetFixture(owner,mutation){
 const raw=fs.readFileSync(B2+'/payload/root/'+owner+'run.mjs','utf8'),ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',files=new Map(),blobs=new Map(),commit='a'.repeat(40),head='b'.repeat(40),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
 const manifest=Buffer.from(JSON.stringify({files:[{file:'run.mjs',bytes:Buffer.byteLength(raw),sha256:sha(Buffer.from(raw))},{file:'all-original-helper.txt',bytes:8,sha256:sha(Buffer.from('COMPLETE'))}]}));
 for(const[f,b]of[['MANIFEST.json',manifest],['run.mjs',Buffer.from(raw)],['all-original-helper.txt',Buffer.from('COMPLETE')]]){files.set(ROOT+'/'+owner+f,b);for(const r of[commit,head,''])blobs.set(r+':'+owner+f,b);}
 let reads=0;const fakeFs={realpathSync:{native:x=>x},lstatSync:p=>({isFile:()=>files.has(p),isSymbolicLink:()=>false,size:files.get(p)?.length??0}),readFileSync:p=>{reads++;assert(files.has(p),'MISSING_COMPLETE_FILE');return files.get(p);}};
 const execFileSync=(_bin,args)=>{const a=args.slice(args.indexOf('-C')+2);if(a[0]==='merge-base')return Buffer.alloc(0);assert.equal(a[0],'show');assert(blobs.has(a[1]),'MISSING_REAL_IMAGE');return blobs.get(a[1]);};
 mutation?.({files,blobs,ROOT,owner,commit,head});const predicate=exactPacketPredicate(raw,{fs:fakeFs,path,crypto,execFileSync,process:{env:{}},fileURLToPath,pathToFileURL:x=>x});return{run:()=>predicate(owner,commit,head),reads:()=>reads};
}
for(const owner of owners){
 test(owner+' actual complete packet predicate reads every manifest row plus raw MF',()=>{const f=packetFixture(owner);f.run();assert.equal(f.reads(),4);});
 for(const mode of['missingHelper','index','configuredOldManifest','head'])test(owner+' packet refusal '+mode,()=>{const f=packetFixture(owner,({files,blobs,ROOT,owner,commit,head})=>{if(mode==='missingHelper')files.delete(ROOT+'/'+owner+'all-original-helper.txt');else blobs.set((mode==='index'?'':mode==='head'?head:commit)+':'+owner+(mode==='configuredOldManifest'?'MANIFEST.json':'all-original-helper.txt'),Buffer.from('foreign'));});assert.throws(f.run,/ADAPTER_(?:BOUNDED_REGULAR_FILE_REQUIRED|COMPLETE_SOURCE_PACKET_IMAGES_REQUIRED)/);});
}
test('public admission refuses incomplete/extra config before any actual source/Git access',async()=>{for(const a of[{},null,{operatorCommit:'HEAD'},{operatorCommit:'a'.repeat(40),extra:true}])await assert.rejects(assertPublicSource(a));});
