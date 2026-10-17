// Manifest + exact public source images only. No sourcePreflight, runtime/native/auth/private access.
import fs from 'node:fs';import cp from 'node:child_process';import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';import assert from 'node:assert/strict';
const read=f=>fs.readFileSync(new URL(f,import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex'),f=JSON.parse(read('FORWARD.json')),m=JSON.parse(read('MANIFEST.json'));
for(const r of m.files){const b=read(r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}
const git=(...a)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',f.root,...a],{windowsHide:true,maxBuffer:4e6,stdio:['ignore','pipe','ignore']});
const head=git('rev-parse','HEAD').toString().trim();git('merge-base','--is-ancestor',f.parentCommit,head);
for(const r of f.all52ParentImages){const b=fs.readFileSync(f.root+'/'+r.target);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);for(const ref of [head,f.parentCommit,''])assert(b.equals(git('show',ref+':'+r.target)));}
assert.equal(git('rev-parse','HEAD').toString().trim(),head);
for(const file of ['payload/input-builder.mjs','payload/result-validator.mjs','test.mjs','verify.mjs','prepare.mjs','seal.mjs'])cp.execFileSync(process.execPath,['--check',fileURLToPath(new URL(file,import.meta.url))],{windowsHide:true,stdio:['ignore','pipe','ignore']});
console.log(JSON.stringify({status:'PASS_SOURCE_ONLY',manifestFiles:m.files.length,parentImages:52,unselectedUnchanged:46,syntax:6,noRuntimeOrNative:true,currentHead:head}));
