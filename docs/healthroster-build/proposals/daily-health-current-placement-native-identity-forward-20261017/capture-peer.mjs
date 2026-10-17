import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='a1845f42868ad8479f524aa4feecf5fd3ce8dbb8',prefix='docs/healthroster-build/proposals/daily-health-current-placement-independent-peer-20261017/',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function get(f){const r=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',repo,'show',commit+':'+prefix+f],{maxBuffer:20e6});assert.equal(r.status,0);return r.stdout;}
function save(f,b){const p=path.join(here,'reference/peer',f);assert(!fs.existsSync(p));fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,b);}
const manifest=get('MANIFEST.json');assert.equal(sha(manifest),'57e65f9e20ccb517063c933f9f40507012338649a3764b1be1a1055128d1023f');save('MANIFEST.json',manifest);
for(const f of ['FINDINGS.json','REVIEW.md','PREDICATE_GAP_RESULTS.json','NATIVE_SOURCE_PINS.json'])save(f,get(f));
const native=JSON.parse(get('NATIVE_SOURCE_PINS.json'));for(const row of native.inputs){const b=get(row.captured);assert.equal(b.length,row.bytes);assert.equal(sha(b),row.sha256);save(row.captured,b);}
console.log(JSON.stringify({peerCommit:commit,nativeHealthCaptures:native.inputs.length,native:'NOT_RUN'}));
