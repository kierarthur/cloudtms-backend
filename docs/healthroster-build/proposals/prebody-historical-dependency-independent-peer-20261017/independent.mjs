// Finite source-only peer checks. No temporary SQL, native/client/auth or private data.
import './frozen/overlay.mjs';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';import {createHash} from 'node:crypto';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',read=f=>fs.readFileSync(here+'/frozen/'+f),sha=b=>createHash('sha256').update(b).digest('hex'),canon=b=>b.toString().replaceAll('\r\n','\n');
const {prebodyDependencySource}=await import(pathToFileURL(root+'/scripts/healthroster-prebody-dynamic-review.mjs').href),recon=JSON.parse(read('SOURCE_RECONSTRUCTIONS.json')),proof=JSON.parse(read('FULL_HELPER_REVERSES.json'));
let wholeInverses=0,anchorRefusals=0,foreignNoTranslations=0,idempotent=0;
for(const row of recon.files){
 const current=read('reference/current/'+row.file),historical=read('reference/historical/'+row.file),input={file:row.file,sha256:row.priorSha256};
 assert.equal(prebodyDependencySource(input,current),canon(historical));assert.equal(prebodyDependencySource(input,prebodyDependencySource(input,current)),canon(historical));idempotent++;
 for(const h of row.hunks)for(const replacement of ['/* independent missing anchor */',h.after+h.after]){
  const source=canon(current);assert.equal(source.split(h.after).length,2);const changed=source.replace(h.after,()=>replacement);assert.throws(()=>prebodyDependencySource(input,changed),/BOUND_SOURCE_CHANGED/);anchorRefusals++;
 }
 for(const altered of [{...input,file:input.file+'.foreign'},{...input,sha256:'f'.repeat(64)}]){assert.equal(prebodyDependencySource(altered,current),canon(current));foreignNoTranslations++;}
}
for(const r of proof.files){const old=read(r.beforePath),next=read(r.afterPath);let source=next.toString();for(const h of [...r.hunks].reverse()){assert.equal(source.split(h.after).length,2);source=source.replace(h.after,()=>h.before);}assert.equal(source,old.toString());assert.equal(sha(source),r.beforeSha256);wholeInverses++;}
const capture=JSON.parse(fs.readFileSync(here+'/CAPTURE.json'));for(const r of capture.rows){const b=read(r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}
assert.equal(wholeInverses,3);assert.equal(anchorRefusals,14);assert.equal(foreignNoTranslations,6);assert.equal(idempotent,3);
console.log(JSON.stringify({status:'INDEPENDENT_FINITE_SOURCE_CHECKS_PASS',authorFiles:28,wholeHelperRawInverses:wholeInverses,changedOrDuplicateAnchorRefusals:anchorRefusals,foreignPathsOrHashesUntranslated:foreignNoTranslations,historicalIdempotence:idempotent,originalNative0400:'NOT_RUN',fullGlobalGate:'NOT_RUN',privateStateAccess:'NONE'}));
