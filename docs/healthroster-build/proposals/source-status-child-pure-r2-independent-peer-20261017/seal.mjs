import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import cp from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),relative=path.relative(repo,here).replaceAll('\\','/'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const walk=p=>fs.readdirSync(p,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(p,e.name)):[path.join(p,e.name)]);
const files=walk(here).filter(f=>path.basename(f)!=='MANIFEST.json').map(f=>{const b=fs.readFileSync(f);return {file:path.relative(here,f).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};}).sort((a,b)=>a.file.localeCompare(b.file));
const target=path.join(here,'MANIFEST.json');
if(process.argv.includes('--verify')){
 const m=JSON.parse(fs.readFileSync(target));assert.deepEqual(files,m.files);
 if(process.argv.includes('--staged'))for(const r of [...files,{file:'MANIFEST.json'}]){const result=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',repo,'show',':'+relative+'/'+r.file],{maxBuffer:20e6});assert.equal(result.status,0,result.stderr.toString());assert(result.stdout.equals(fs.readFileSync(path.join(here,r.file))),r.file);}
 console.log(JSON.stringify({files:files.length+1,manifestSha256:sha(fs.readFileSync(target)),savedIndex:process.argv.includes('--staged')?'EQUAL':'NOT_CHECKED',native:'NOT_RUN'}));
}else{assert(!fs.existsSync(target));fs.writeFileSync(target,JSON.stringify({version:'SOURCE_STATUS_CHILD_PURE_R2_INDEPENDENT_PEER_MANIFEST_V1',authorCommit:'d56db402e8b415a1303b0ae5956ceab2ec6822ca',verdict:'ACCEPT_BOUNDED_SOURCE_CORRECTION',native:'NOT_RUN',restoration:'NOT_RUN',files},null,2)+'\n');console.log(JSON.stringify({files:files.length+1,manifestSha256:sha(fs.readFileSync(target))}));}
