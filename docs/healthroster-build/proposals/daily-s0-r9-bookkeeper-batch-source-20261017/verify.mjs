// Read-only source verification; never imports/runs the bookkeeper or private source preflight.
import fs from 'node:fs';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import cp from 'node:child_process';import {fileURLToPath} from 'node:url';
const read=f=>fs.readFileSync(new URL(f,import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex'),m=JSON.parse(read('MANIFEST.json')),pins=JSON.parse(read('SOURCE_PINS.json'));
const git=(r,...a)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',r,...a],{windowsHide:true,maxBuffer:8e6,stdio:['ignore','pipe','pipe'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
for(const r of m.files){const b=read(r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}
for(const r of pins.pins){const b=fs.readFileSync(r.root+'/'+r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);for(const ref of[r.commit,'HEAD',''])assert(b.equals(git(r.root,'show',ref+':'+r.file)));}
for(const f of['bookkeeper.mjs','test.mjs','measure-readonly.mjs','verify.mjs'])cp.execFileSync(process.execPath,['--check',fileURLToPath(new URL(f,import.meta.url))],{windowsHide:true,stdio:['ignore','pipe','pipe']});
console.log(JSON.stringify({status:'PASS_BOUNDED_SOURCE_ONLY',manifestFiles:m.files.length,sourcePins:pins.pins.length,grammar:4,realBookkeeper:'NOT_RUN',nativeAuthPrivateRootWrites:'NONE'}));
