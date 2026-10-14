import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
import * as current from './source-equivalence.mjs';import * as former from '../strict-qualification-first-use-inventory-forward/source-equivalence.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),root=path.resolve(own,'../healthroster-v2-backend-13102026');
const commit='cea15289e0283e3383e71f0acb8b18ce184647c2';
const raw=execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','ls-tree','-r','-z',commit],{cwd:root,maxBuffer:2_000_000});
const id=former.parseGitTree(raw).find(row=>row.path==='.node-version').blob;
const entry=(name,kind='100644 blob')=>Buffer.concat([Buffer.from(`${kind} ${id}\t`),Buffer.isBuffer(name)?name:Buffer.from(name),Buffer.from('\0')]);
const withEntry=(name,kind)=>Buffer.concat([raw,entry(name,kind)]);
const nativeCode=fs.readFileSync(path.join(out,'weekly-source-protected-execution-tree.mjs'),'utf8').replace("'./weekly-source-protected-release-qualification.mjs'",JSON.stringify(pathToFileURL(path.resolve(out,'../protected-source-capability/release-qualification.mjs')).href));
const native=await import('data:text/javascript;base64,'+Buffer.from(nativeCode).toString('base64'));

test('lawful actual complete before/after proposal/native proof and full DTO mapper are byte identical',()=>{
 const before=former.proveCommittedSources(root,commit,commit),after=current.proveCommittedSources(root,commit,commit),nativeAfter=native.proveCommittedSources(root,commit,commit);
 assert.deepEqual(after,before);assert.deepEqual(nativeAfter,after);
 assert.deepEqual(current.assertExecutionTreeProof(after,commit,commit),former.assertExecutionTreeProof(before,commit,commit));
 assert.deepEqual(native.assertExecutionTreeProof(nativeAfter,commit,commit),after);
 assert.equal(after.files.length,3694);assert.equal(Buffer.byteLength(JSON.stringify(after)),2031987);
 assert.equal(after.source_equivalence_sha256,'8a5d6071b8dae6e9eea50d0bffc6a7c5459139440b4738ab0da8ef20a6096279');
});
test('exact unsupported-backslash row and unsupported object refuse before selection',()=>{
 for(const kind of ['100644 blob','160000 commit']){
  const input=withEntry('unsupported\\source-input.sql',kind);assert.equal(former.parseGitTree(input).length,3694);
  for(const parser of [current,native])assert.throws(()=>parser.parseGitTree(input),/QUALIFICATION_GIT_PATH_UNSUPPORTED/);
 }
});
test('exact FF/FE lossy-name counterexamples refuse instead of collapsing',()=>{
 const a=withEntry(Buffer.concat([Buffer.from('input-'),Buffer.from([0xff]),Buffer.from('.txt')]));
 const b=withEntry(Buffer.concat([Buffer.from('input-'),Buffer.from([0xfe]),Buffer.from('.txt')]));
 assert.deepEqual(former.parseGitTree(a),former.parseGitTree(b));
 for(const parser of [current,native])for(const input of [a,b])assert.throws(()=>parser.parseGitTree(input),/QUALIFICATION_GIT_PATH_UTF8_INVALID/);
});
test('valid Unicode raw pathname roundtrip remains exact and Unicode path drift refuses',()=>{
 const a=current.parseGitTree(withEntry('source-é.txt')),b=current.parseGitTree(withEntry('source-è.txt'));
 assert.equal(a.length,3695);assert.equal(a.find(row=>row.path==='source-é.txt').blob,id);
 assert.deepEqual(native.parseGitTree(withEntry('source-é.txt')),a);
 const read=oid=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','cat-file','blob',oid],{cwd:root,maxBuffer:20_000_000});
 // The changed path sorts late. Compare the isolated two real, valid Git rows
 // so this vector fails on the exact path identity before any blob read.
 assert.throws(()=>current.assertTreeEquivalence([a.find(row=>row.path==='source-é.txt')],[b.find(row=>row.path==='source-è.txt')],read),/QUALIFICATION_SOURCE_MODE_TYPE_OR_PATH_CHANGED/);
});
test('no internal empty record, unsupported path or missing terminal NUL can be omitted',()=>{
 for(const input of [Buffer.concat([raw,Buffer.from('\0')]),raw.subarray(0,raw.length-1)]){
  assert.throws(()=>current.parseGitTree(input),/QUALIFICATION_GIT_TREE_INVALID/);
 }
 for(const name of ['../outside.sql','a/../b.sql','/absolute.sql','a//b.sql'])assert.throws(()=>current.parseGitTree(withEntry(name)),/QUALIFICATION_GIT_PATH_UNSUPPORTED/);
});
