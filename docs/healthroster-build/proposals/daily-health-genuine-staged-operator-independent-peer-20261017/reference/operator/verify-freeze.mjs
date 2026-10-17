// Safe read-only verifier: proposal source only. Does not invoke sourcePreflight or inspect private state.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';import {sha} from './contract.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../..'),relative=path.relative(root,here).replaceAll('\\','/');
const bytes=fs.readFileSync(here+'/MANIFEST.json'),manifest=JSON.parse(bytes),seen=[];
function walk(dir,base=''){for(const e of fs.readdirSync(dir,{withFileTypes:true})){assert(!e.isSymbolicLink());const r=base+e.name;if(e.isDirectory())walk(path.join(dir,e.name),r+'/');else if(r!=='MANIFEST.json')seen.push(r);}}walk(here);
assert.deepEqual(seen.sort(),manifest.files.map(r=>r.file).sort());
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6,windowsHide:true,stdio:['ignore','pipe','ignore']});
const committed=process.argv.includes('--committed');
for(const r of [...manifest.files,{file:'MANIFEST.json',bytes:bytes.length,sha256:sha(bytes)}]){const b=fs.readFileSync(here+'/'+r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);if(committed){assert(b.equals(git('show','HEAD:'+relative+'/'+r.file)));assert(b.equals(git('show',':'+relative+'/'+r.file)));}}
console.log(JSON.stringify({files:manifest.files.length+1,manifestSha256:sha(bytes),savedImages:'PASS',indexHeadImages:committed?'PASS':'NOT_CHECKED',privateStateAccess:false,native:'NOT_RUN'}));
