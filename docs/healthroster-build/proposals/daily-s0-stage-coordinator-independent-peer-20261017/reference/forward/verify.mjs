// Saved public source only. Never calls source()/sourcePreflight(), state, IPC or an operator.
import fs from 'node:fs';import crypto from 'node:crypto';
const dir=new URL('./',import.meta.url),hash=b=>crypto.createHash('sha256').update(b).digest('hex'),manifest=JSON.parse(fs.readFileSync(new URL('MANIFEST.json',dir)));
for(const r of manifest.files){const b=fs.readFileSync(new URL(r.file,dir));if(b.length!==r.bytes||hash(b)!==r.sha256)throw Error('S0_STAGE_FROZEN_SOURCE_CHANGED');}
console.log(JSON.stringify({status:'PASS',frozenFiles:manifest.files.length,manifestSha256:hash(fs.readFileSync(new URL('MANIFEST.json',dir))),native:'NOT_RUN',browser:'NOT_RUN',privateState:'NOT_ACCESSED'}));
