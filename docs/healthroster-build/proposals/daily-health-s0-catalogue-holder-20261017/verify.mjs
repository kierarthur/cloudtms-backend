// Public saved source only; never a preflight, credential/state read, process launch or native call.
import fs from 'node:fs';import crypto from 'node:crypto';
const dir=new URL('./',import.meta.url),hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const manifest=JSON.parse(fs.readFileSync(new URL('MANIFEST.json',dir)));
for(const r of manifest.files){const b=fs.readFileSync(new URL(r.file,dir));if(b.length!==r.bytes||hash(b)!==r.sha256)throw Error('S0_FROZEN_SOURCE_CHANGED');}
console.log(JSON.stringify({status:'PASS',frozenFiles:manifest.files.length,manifestSha256:hash(fs.readFileSync(new URL('MANIFEST.json',dir))),native:'NOT_RUN',compatibility:'NOT_QUALIFIED'}));
