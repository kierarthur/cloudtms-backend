import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex'),m=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));
const walk=(dir,p='')=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name),p+e.name+'/'):[p+e.name]).sort();assert.deepEqual(m.files.map(r=>r.file).sort(),walk(here).filter(f=>f!=='MANIFEST.json'));
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',here,...args],{windowsHide:true,maxBuffer:16*1024*1024});
const repo=git('rev-parse','--show-toplevel').toString().trim(),prefix=path.relative(repo,here).replaceAll('\\','/')+'/';
for(const r of [...m.files,{file:'MANIFEST.json'}]){const b=fs.readFileSync(here+'/'+r.file);if(r.sha256){assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}if(process.argv.includes('--committed'))for(const rev of ['HEAD',''])assert(b.equals(git('show',rev+':'+prefix+r.file)),r.file);}
console.log(JSON.stringify({files:m.files.length+1,manifestSha256:sha(fs.readFileSync(here+'/MANIFEST.json')),verdict:m.verdict,native:'NOT_RUN'}));
