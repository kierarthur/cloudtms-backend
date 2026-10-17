// Read-only exact public inventory/full-source inverses. No source()/holder/root IO execution.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash}from'node:crypto';
const A='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/qt-audit-readonly-cost1000-source-20261017-25d4b4a24cb8b99631c33f79550a809a-closed-failure-forward-stream-forward';
const P=A.replace(/-stream-forward$/,'');const hash=b=>createHash('sha256').update(b).digest('hex');const read=(p)=>fs.readFileSync(p);const json=p=>JSON.parse(read(p));
assert.equal(hash(read(A+'/MANIFEST.json')),'3177911190336340676bb1fbd6756bc2b193850f8722ece225a9d9865c8e8344');
assert.equal(hash(read(P+'/MANIFEST.json')),'ed10acad577a90a40ea5aa31749933ad725755e09ae4efb864198a1e9b8f53eb');
const walk=d=>fs.readdirSync(d,{withFileTypes:true}).filter(e=>e.name!=='__pycache__').flatMap(e=>e.isDirectory()?walk(path.join(d,e.name)):[path.join(d,e.name)]);
for(const dir of[A,P]){const m=json(dir+'/MANIFEST.json');const files=walk(dir).filter(p=>path.resolve(p)!==path.resolve(dir+'/MANIFEST.json')).map(p=>path.relative(dir,p).replaceAll('\\','/')).sort();assert.deepEqual(files,m.files.map(e=>e.file).sort());for(const e of m.files){const b=read(dir+'/'+e.file);assert.equal(b.length,e.bytes,e.file);assert.equal(hash(b),e.sha256,e.file);}}
const forward=json(A+'/STREAM_FORWARD.json');let controller=read(A+'/root-tools.mjs').toString();for(const e of [...forward.runtimeEdits].reverse()){assert.equal(controller.split(e.after).length,2);controller=controller.replace(e.after,()=>e.before);}assert.deepEqual(Buffer.from(controller),read(P+'/root-tools.mjs'));assert.equal(hash(controller),forward.parentSha256);
let tests=read(A+'/root-tools.test.mjs').toString();assert.equal(tests.split(forward.testFixture.after).length,2);tests=tests.replace(forward.testFixture.after,()=>forward.testFixture.before);assert.deepEqual(Buffer.from(tests),read(P+'/root-tools.test.mjs'));
for(const e of json(P+'/MANIFEST.json').files){if(['root-tools.mjs','root-tools.test.mjs','FAILURE_FORWARD.json','RESULTS.json'].includes(e.file))continue;assert.deepEqual(read(A+'/'+e.file),read(P+'/'+e.file),e.file);}
assert.deepEqual(read(A+'/history/ED10_MANIFEST.json'),read(P+'/MANIFEST.json'));
console.log(JSON.stringify({status:'INDEPENDENT_COMPLETE_SOURCE_INVERSE_PASS',currentFiles:46,heldParentFiles:33,runtimeInverseHunks:3,testFixtureInverseHunks:1,heldParentUnchanged:true,sqlHolderValidatorSourceBudgetBodies:'RAW_EQUAL',native:'NOT_RUN'}));
