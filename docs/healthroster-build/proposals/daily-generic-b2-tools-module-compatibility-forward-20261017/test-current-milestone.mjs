// Synthetic frames only. Imports the explicitly supplied PURE MODULE MILESTONE,
// never a CLI, source admission, native provider, actual output, or private path.
import fs from 'node:fs';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {pathToFileURL} from 'node:url';
const moduleUrl=new URL('./reference/milestone/current-read.mjs',import.meta.url),expected='db5332b466f979fd78e1e9d58477990d72e012c2f7d94ea84324b204e53471fc',sha=b=>createHash('sha256').update(b).digest('hex');
assert.equal(sha(fs.readFileSync(moduleUrl)),expected,'milestone changed; require explicit new author pin');
for(const row of JSON.parse(fs.readFileSync(new URL('./MILESTONE_CAPTURE.json',import.meta.url))).files){const b=fs.readFileSync(new URL('./reference/milestone/'+row.file,import.meta.url));assert.equal(b.length,row.bytes);assert.equal(sha(b),row.sha256);}
const currentModule=await import(moduleUrl.href),native=await import(new URL('native-read.mjs',moduleUrl)),contract=await import(new URL('contract.mjs',moduleUrl)),source='1'.repeat(40);
const body=fs.readFileSync(new URL('./controller.js.txt',import.meta.url),'utf8'),AF=Object.getPrototypeOf(async function(){}).constructor,out='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/.codex-tmp/daily-genuine-stages-20261017-'+'1'.repeat(32);
function actualFixture(stage='S2'){
 const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:71,backendPid:701,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1],takenAt:'2026-10-17T10:00:00.000Z',task:'DAILY_GENUINE_'+stage+'_STAGED_ONLY'},actorId='e7d84371-0000-0000-0000-000000000001',generated={...stage!=='S1'?{settingsId:'e7d84371-0000-0000-0000-000000000002'}:{},...stage==='S4'?{rateId:'e7d84371-0000-0000-0000-000000000003'}:{}};
 const beforeBuilt=native.buildReadInput({stage,phase:'BEFORE',identity,actorId,...generated}),beforeInputBytes=currentModule.jsonBytes(beforeBuilt),beforeBytes=Buffer.from('SYNTHETIC_HASH_PREIMAGE_ONLY_NOT_A_NATIVE_ENVELOPE'),entry={sourceCommit:source,beforeSqlSha256:beforeBuilt.receipt.sqlSha256,verificationSqlSha256:beforeBuilt.receipt.verificationSqlSha256,beforeEnvelopeSha256:sha(beforeBytes),effectAdmissionSha256:'c'.repeat(64)},entrySha256=sha(contract.entryText(stage,entry));
 const pending=currentModule.buildCurrent({stage,identity,actorId,generated,beforeBuilt,beforeInputBytes,beforeBytes,entry,entrySha256,admittedAt:'2026-10-17T10:00:01.000Z',readyAt:'2026-10-17T10:00:02.000Z',nonce:'b'.repeat(32)}),f={stage,pending,beforeBuilt},names=currentModule.currentFiles(stage),files=new Map([[names.input,currentModule.jsonBytes(pending.built)],[names.binding,currentModule.jsonBytes(pending.binding)]]);
 return {...f,names,files,ready:structuredClone(f.pending.ready)};
}
async function load(f,{validator=currentModule}={}){
 const fragment=body.slice(body.indexOf(' async function current(readyFrame)'),body.indexOf(' async function bookkeep(phase)'));let program;
 await assert.rejects(new AF('readyFrame','state','stage','K4','report','P','node',fragment+'return await current(readyFrame);')(f.ready,{out},f.stage,source,'REPORT','docs/healthroster-build/proposals/',async s=>{program=s;throw Error('CAPTURE_ONLY')}),/CAPTURE_ONLY/);
 program=program.replace('import fs from "node:fs";import{createHash}from"node:crypto";import{pathToFileURL}from"node:url";','').replace('await import(pathToFileURL(report+"/"+P+"daily-health-genuine-staged-operator-20261017/current-read.mjs").href)','currentModule');
 let output='';const file=x=>{assert(x.startsWith(out+'/'));const n=x.slice(out.length+1);assert(f.files.has(n));return f.files.get(n);};
 const memoryFs={realpathSync:{native:x=>x},lstatSync:x=>({isFile:()=>true,isSymbolicLink:()=>false,size:file(x).length}),readFileSync:file};
 await new AF('fs','createHash','pathToFileURL','process','currentModule',program)(memoryFs,createHash,pathToFileURL,{stdout:{write:v=>output+=v}},validator);
 return output;
}
for(const stage of ['S1','S1a','S2','S3','S4'])test('actual milestone buildCurrent/assertCurrentReady plus generated transport: '+stage,async()=>{
 const f=actualFixture(stage),raw=await load(f);assert.deepEqual(JSON.parse(raw),f.pending.built);assert.equal(Buffer.byteLength(raw),currentModule.jsonBytes(f.pending.built).length);
 assert.equal(Object.keys(f.pending.binding).length,16);assert.equal(Object.keys(f.pending.binding.identity).length,7);assert.equal(f.pending.built.receipt.phase,'BEFORE');
 const original=structuredClone(f.beforeBuilt);original.input.label=f.pending.built.input.label;original.receipt.label=f.pending.built.receipt.label;assert.deepEqual(f.pending.built,original);
});
test('actual ready validator refuses source, stage, task, key count, nonce and date substitutions before any memory file read',async()=>{
 for(const mutate of [r=>r.binding.operatorSourceCommit='2'.repeat(40),r=>r.binding.stage='S3',r=>r.binding.identity.task='DAILY_GENUINE_S3_STAGED_ONLY',r=>r.binding.extra=true,r=>r.binding.nonce='B'.repeat(32),r=>r.binding.readyAt='impossible']){
  const f=actualFixture();mutate(f.ready);await assert.rejects(load(f));
 }
});
test('actual milestone raw bindings and pair bytes cannot substitute under valid ready hashes',async()=>{
 for(const key of ['input','binding']){const f=actualFixture();f.files.set(f.names[key],Buffer.from('{}\n'));await assert.rejects(load(f),/ROOT_B2_RAW_READY_HASH/);}
});
test('actual ready binding is bounded and cannot use an arbitrary extra progress field',async()=>{
 for(const mutate of [r=>r.extra=true,r=>r.inputSha256='not-a-sha',r=>r.bindingSha256='0'.repeat(64)]){const f=actualFixture();mutate(f.ready);await assert.rejects(load(f));}
});
test('module milestone remains the exact author supplied image after synthetic replay',()=>{assert.equal(sha(fs.readFileSync(moduleUrl)),expected);});
