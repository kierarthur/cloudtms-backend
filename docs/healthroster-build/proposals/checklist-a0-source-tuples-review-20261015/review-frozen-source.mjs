// Independent immutable tuple-successor source review. No generator or SQL.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import test from 'node:test';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='324e3f1c3d819170aab5b319334eeb9a2af7c2d1';
const tuples='docs/healthroster-build/proposals/a0-source-selector-tuples-forward-20261014/';
const core='docs/healthroster-build/proposals/a0-source-selector-domain-construction-20261014/';
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const git=(rev,file)=>execFileSync('git',['-c','maintenance.auto=false','cat-file','blob',rev+':'+file],{cwd:repo,maxBuffer:5e6});
const blobs=new Map(),pins=[],originalGit=new Map();
const capture=(rev,file)=>{const raw=git(rev,file);blobs.set(path.join(repo,file),raw);pins.push({commit:rev,file,bytes:raw.length,sha256:sha(raw)});return raw;};
const changed=execFileSync('git',['diff-tree','--no-commit-id','--name-only','-r',commit],{cwd:repo,encoding:'utf8'}).trim().split('\n');
for(const file of changed)capture(commit,file);
const coreFiles=execFileSync('git',['ls-tree','-r','--name-only',commit,'--',core],{cwd:repo,encoding:'utf8'}).trim().split('\n');
for(const file of coreFiles)capture(commit,file);
const scannerBytes=capture(commit,'scripts/cloudtms-managed-release-sql.mjs');
const {scanManagedSql}=await import('data:text/javascript;base64,'+scannerBytes.toString('base64'));
for(const base of [tuples,core])for(const r of JSON.parse(blobs.get(path.join(repo,base+'COMPLETE_CURRENT_REVERSES.json'))).records){
 const key=r.source_commit+':'+r.source_path;if(!originalGit.has(key))originalGit.set(key,capture(r.source_commit,r.source_path));}
const local=p=>p instanceof URL?fileURLToPath(p):String(p);
const frozenFs={readFileSync(p,encoding){const raw=blobs.get(path.resolve(local(p)));assert.ok(raw,'Unpinned source read: '+local(p));return encoding?raw.toString(encoding):Buffer.from(raw);},
 readdirSync(p){const dir=path.resolve(local(p));assert.ok([tuples,core].some(base=>dir===path.join(repo,base,'forward')));return [...blobs.keys()].filter(file=>path.dirname(file)===dir).map(file=>path.basename(file));}};
const frozenExec=(command,args)=>{assert.equal(command,'git');assert.equal(args[0],'cat-file');assert.equal(args[1],'blob');assert.ok(originalGit.has(args[2]));return Buffer.from(originalGit.get(args[2]));};
const adaptations=[];
for(const file of ['tests/healthroster-a0-source-selector-domain.test.mjs','tests/healthroster-a0-source-selector-tuples.test.mjs']){
 const raw=blobs.get(path.join(repo,file))??capture(commit,file);let source=raw.toString().replace(/^import .*;\r?\n/gm,'');
 source=source.replace('import.meta.url',JSON.stringify(new URL('file:///'+path.join(repo,file).replaceAll('\\','/')).href));assert.ok(!source.includes('import.meta'));
 new vm.Script(source,{filename:commit+':'+file}).runInContext(vm.createContext({fs:frozenFs,execFileSync:frozenExec,scanManagedSql,
  test,assert,createHash:crypto.createHash,URL,Buffer,console}));
 adaptations.push({file,original_sha256:sha(raw),harness_source_sha256:sha(source),changes:'ESM imports bound to pinned scanner/built-ins and import.meta URL fixed to immutable virtual source. All test bodies and assertions unchanged.'});}
const lexical=[tuples,core].flatMap(base=>[...blobs.entries()].filter(([file])=>path.dirname(file)===path.join(repo,base,'forward')).map(([file,raw])=>{
 const p=scanManagedSql(raw.toString());assert.equal(p.meta.length,0);return {file:path.relative(repo,file).replaceAll('\\','/'),statements:p.statements.length,sha256:sha(raw)};}));
for(const base of [tuples,core])assert.equal(lexical.filter(x=>x.file.startsWith(base)).reduce((n,x)=>n+x.statements,0),31);
const root='docs/healthroster-build/root-a0-source-selector-tuples-source-20261015/';
for(const m of JSON.parse(blobs.get(path.join(repo,root+'SHA256_MANIFEST.json'))).artifacts){const raw=blobs.get(path.join(repo,root+m.file));assert.equal(raw.length,m.bytes);assert.equal(sha(raw),m.sha256);}
fs.writeFileSync(path.join(here,'FROZEN_REVIEW_INPUT_PINS.json'),JSON.stringify({status:'BOUNDED_INDEPENDENT_TUPLE_SOURCE_REVIEW',commit,
 SQL_executed:false,native:false,generators_executed:false,another_lane_edited:false,pins,lexical,adaptations},null,2)+'\n');
