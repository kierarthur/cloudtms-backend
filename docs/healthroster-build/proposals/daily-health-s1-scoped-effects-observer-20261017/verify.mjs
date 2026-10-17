import fs from 'node:fs';import path from 'node:path';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex');
const mf=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));for(const r of mf.files){const b=fs.readFileSync(here+'/'+r.file);assert.equal(b.length,r.bytes,r.file);assert.equal(sha(b),r.sha256,r.file);}
const env={...process.env,PYTHONDONTWRITEBYTECODE:'1'};
for(const [cmd,args]of [['node',[here+'/capture.mjs']],['node',['--test',here+'/observer.test.mjs']],['python',[here+'/grammar.py']]])process.stdout.write(execFileSync(cmd,args,{env,maxBuffer:2*1024*1024}));
console.log(JSON.stringify({verdict:'SOURCE_ONLY_OBSERVER_PROPOSAL',frozenFiles:mf.files.length+1,productTargets:0,native:'NOT_RUN',completeCallbackEffects:'NOT_PROVED',restoration:'NOT_A_RESTORATION_PROGRAMME'}));
