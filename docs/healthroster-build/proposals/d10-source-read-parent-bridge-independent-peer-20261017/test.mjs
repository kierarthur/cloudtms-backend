import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
const author='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/.codex-tmp/d10-source-read-parent-bridge-20261017';
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const read=p=>fs.readFileSync(path.join(author,p));
const json=p=>JSON.parse(read(p));
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true',...a],{cwd:root,env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'},maxBuffer:32*1024*1024});
const forward=json('FULL_PARENT_FORWARD.json'),selected=json('PAYLOAD_SELECTION.json'),parents=json('PARENTS.json');
test('every complete before image is cross-bound to capture, selection, actual saved/index and declared historical HEAD',()=>{
 for(const f of forward.rows){
  const s=selected.targets.find(x=>x.target===f.target),p=parents.rows.find(x=>x.target===f.target),captured=read('parents/'+f.target);
  assert.equal(f.before,captured.toString());assert.equal(f.beforeBytes,captured.length);assert.equal(f.beforeSha256,sha(captured));
  assert.equal(s.beforeBytes,captured.length);assert.equal(s.beforeSha256,sha(captured));
  assert.deepEqual(fs.readFileSync(path.join(root,f.target)),captured);assert.deepEqual(git('show',':'+f.target),captured);
  assert.equal(sha(captured),p.savedAndIndexSha256);
  if(p.historicalSha256)assert.equal(sha(git('show',p.HEAD+':'+f.target)),p.historicalSha256);
  else assert.throws(()=>git('cat-file','-e',p.HEAD+':'+f.target));
 }
});
test('complete exact inverses reject a snippet parent and reconstruct every raw byte',()=>{
 for(const f of forward.rows){
  let source=f.after;for(const h of [...f.hunks].reverse()){
   if(h.kind==='APPEND'){assert.ok(source.endsWith(h.after));source=source.slice(0,-h.after.length);}
   else {assert.equal(source.split(h.after).length,2);source=source.replace(h.after,()=>h.before);}
  }
  assert.equal(source,f.before);assert.equal(sha(source),f.beforeSha256);assert.equal(Buffer.byteLength(source),f.beforeBytes);
  assert.notEqual(sha(f.before.slice(0,158)),f.beforeSha256);
 }
});
test('new module is a pure append to the whole original module; old factory/proofs raw unchanged',()=>{
 const f=forward.rows.find(x=>x.target.endsWith('healthroster-d10-reader-type-review.mjs'));
 assert.equal(f.hunks.length,1);assert.equal(f.hunks[0].kind,'APPEND');assert.ok(f.after.startsWith(f.before));
 const p=parents.unchangedSourceRead,b=read(p.source);
 assert.equal(sha(b),p.sha256);assert.deepEqual(b,fs.readFileSync(path.join(root,p.target)));
 assert.deepEqual(b,git('show',':'+p.target));assert.deepEqual(b,git('show','HEAD:'+p.target));
});
test('gate initializer changes retain every original callback/options byte after the one factory argument',()=>{
 const f=forward.rows.find(x=>x.target.endsWith('bpay-next-source-gates.mjs'));
 assert.equal(f.hunks.length,2);
 const start='  hazardCensus:specialConstructViolations,\n  classifyPrior:';
 const before=f.before.slice(f.before.indexOf(start));const after=f.after.slice(f.after.indexOf(start));
 assert.equal(after,before);assert.equal((f.after.match(/sourceReadFactory:createReviewedSourceReadClassifier/g)||[]).length,1);
});
test('all 21 retained proof files equal their real historical and current saved/index images',()=>{
 const rows=json('RETAINED_REAL_PROOF_INPUTS.json').rows;assert.equal(rows.length,21);
 for(const r of rows){const b=read(r.capture);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);
  assert.deepEqual(b,git('show',r.commit+':'+r.file));assert.deepEqual(b,git('show',':'+r.file));assert.deepEqual(b,fs.readFileSync(path.join(root,r.file)));}
});
test('frozen inventory remains exact after author and independent replay',()=>{
 assert.equal(sha(read('MANIFEST.json')),'25a5053d497209028a0c68c45e69d0687596c948d1bc1b6b5221bf8b58153b6f');
 const rows=json('MANIFEST.json').files;assert.equal(rows.length,47);
 for(const r of rows){const b=read(r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}
});
